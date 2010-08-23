# use Devel::Cycle;use ElasticSearch::AnyEvent; $c=ElasticSearch::AnyEvent->new(servers=>['127.0.0.1:9200','127.0.0.1:9201']); $c->trace_calls(0)
# $d=$c->cluster_health
# $cv=AE::cv(); $w=AnyEvent->io(fh=>\*STDIN,poll=>'r',cb=>$cv); $cv->recv; undef $w


package ElasticSearch::AnyEvent;

use strict;
use warnings FATAL => 'all';
use base 'ElasticSearch';
use Scalar::Util();

#===================================
sub current_server {
#===================================
    my $self = shift;
    if (@_) {
        $self->_set_current_server(@_)
    }
    return ElasticSearch::AnyEvent::CondVar->new($self)
        ->refresh_servers( $self->_check_current_server );
}

=item C<timeout()>

=cut

#===================================
sub timeout {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_timeout} = shift;
    }
    return $self->{_timeout} || 0;
}

#===================================
sub _check_current_server { shift->{_current_server}{$$} }
sub _set_current_server { $_[0]->{_current_server} = { $$ => $_[1] } }
#===================================

#===================================
sub refresh_servers {
#===================================
    my $self = shift;
    $self->_set_current_server(undef);
    return ElasticSearch::AnyEvent::CondVar->new($self)->refresh_servers();
}

#===================================
sub request {
#===================================
    my $self   = shift;
    my $params = shift;
    my $method = $params->{method} || 'GET';
    my $cmd    = $params->{cmd}
        || $self->throw( 'Request', "No cmd specified" );

    my $data = $params->{data};
    my $json = $self->JSON;

    if ( defined $data ) {
        $data = $json->encode($data);
        utf8::upgrade($data);
        utf8::encode($data);
    }

    return ElasticSearch::AnyEvent::CondVar->new($self)
        ->failover_request( $method, $cmd, $data );
}

#===================================
#===================================
package ElasticSearch::AnyEvent::CondVar;
#===================================
#===================================

use AnyEvent();
use base 'AnyEvent::CondVar';
use AnyEvent::HTTP qw(http_request);

our $count     = 0;
our $destroyed = 0;

our %Objects;

#===================================
sub new {
#===================================
    my $proto = shift;
    my $class = ref $proto || $proto;
    my $self  = AnyEvent->condvar;
    bless $self, $class;
    $self->{es}      = shift;
    $self->{COUNT}   = ++$count;
    $Objects{$count} = $self;
    Scalar::Util::weaken( $Objects{$count} );
    my ( $c, undef, $l, $s ) = caller(1);
    use Data::Dump qw(pp);
    pp( scalar { 'Creating' => [ $c, $l, $s ] } );
    return $self;
}

#===================================
sub request {
#===================================
    my ( $self, $method, $server, $cmd, $data ) = @_;
    $self->{PARAMS} = [ $method, $server, $cmd, $data ];
    my $cv = $self;
    Scalar::Util::weaken $cv if defined wantarray;

    $self->{guard} = http_request(
        $method => $server . $cmd,
        body    => $data,
        timeout => $self->es->timeout,
        sub {
#                $DB::single=1;
            delete $cv->{guard};
            $cv->es->_log_request( $method, $server, $cmd, $data );
            my ( $result, $error ) = $cv->parse_response( $server, @_ );
            if ($result) {
                return $cv->send($result);
            }
            return $cv->croak($error);
        }
    );
    return $self;
}

#===================================
sub failover_request {
#===================================
    my ( $self, $method, $cmd, $data ) = @_;

    my $cv = $self;
    Scalar::Util::weaken $cv if defined wantarray;

    my ( $request_cb, $server_cb, $server );
    $server_cb = sub {
#                $DB::single=1;
        delete $cv->{guard};

        $server = shift || $cv->croak($@);
        $cv->{guard} = http_request(
            $method => $server . $cmd,
            body    => $data,
        timeout => $self->es->timeout,
            $request_cb
        );
    };

    $request_cb = sub {
#                $DB::single=1;
        delete $cv->{guard};
        $cv->es->_log_request( $method, $server, $cmd, $data );
        my ( $result, $error ) = $cv->parse_response( $server, @_ );
        return $cv->send($result) if $result;

        if ( ref $error
            && $error->isa('ElasticSearch::AnyEvent::Error::Connection') )
        {
            print STDERR "$error\n";
            $cv->{guard} = $cv->es->refresh_servers->cb( $server_cb);
            return;
        }
        $cv->croak($error);
    };
    Scalar::Util::weaken($server_cb);
    Scalar::Util::weaken($request_cb);
    $self->{guard} = $self->es->current_server->cb( $server_cb );
    return $self;
}

=item C<parse_response()>

=cut

our %Connection_Errors = map { $_ => 1 }
    ( 'Connection reset by peer', 'Connection refused', 'Broken pipe' );

#===================================
sub parse_response {
#===================================
    my ( $self, $server, $content, $headers ) = @_;
$DB::single=1;
    $content = '{}' unless defined $content;

    my $es = $self->es;

    my ( $result, $json_error );
    eval { $result = $es->JSON->decode($content); 1 }
        or $json_error = ( $@ || 'Unknown JSON error' );

    $es->_log_result( $result || $content );

    my ( $status, $reason ) = @{$headers}{qw(Status Reason)};
    my $success = $status =~ /^2/;

    if ( $success && !$json_error ) {
        return $result;
    }

    my $error_params = {
        server      => $server,
        response    => $json_error ? $content : $result,
        status_code => $status,
        status_msg  => $reason,
    };

    my ( $error_type, $error_msg );
    if ( $success && $json_error ) {
        $error_type = 'JSON';
        $error_msg  = $json_error;
    }
    else {
        $error_type
            = $reason eq 'Connection timed out' ? 'Timeout'
            : $Connection_Errors{$reason} ? 'Connection'
            :                               'Request';
        $error_msg
            = $result && $result->{error}
            ? $result->{error}
            : $error_params->{status_msg} . ' ('
            . $error_params->{status_code} . ')';
    }

    my $error
        = $self->es->build_error( $error_type, $error_msg, $error_params );
    return ( undef, $error );
}

#===================================
sub refresh_servers {
#===================================
    my $self = shift;
    if ( my $server = shift ) {
        $self->send($server);
        return $self;
    }

    my $es = $self->es;
    my %servers
        = map { $_ => 1 } ( @{ $es->servers }, @{ $es->default_servers } );
    $self->{servers} = \%servers;

    my @all_servers = keys %servers;

    my $cv = $self;
    Scalar::Util::weaken $cv if defined wantarray;

    foreach my $server ( keys %servers ) {
        my $request
            = $cv->new($es)->request( 'GET', $server, '/_cluster/nodes' );
        $servers{$server} = $request;

        $request->cb(
            sub {
#                $DB::single=1;
                if ( my $current = $cv->es->_check_current_server ) {
                    %servers = ();
                    return $cv->send($current);
                }
                return $cv->send unless $servers{$server};
                my $nodes = $_[0] && shift()->{nodes};
                if ($nodes) {
                    my @live_servers;
                    for ( values %$nodes ) {
                        my $server
                            = $_->{http_address}
                            || $_->{httpAddress}
                            || next;
                        $server =~ m{inet\[(\S*)/(\S+):(\d+)\]} or next;
                        push @live_servers,
                            'http://' . ( $1 || $2 ) . ':' . $3;
                    }
                    if (@live_servers) {
                        $es->servers( \@live_servers );
                        %servers = ();
                        my $current_server
                            = $live_servers[ int( rand(@live_servers) ) ];
                        $cv->es->_set_current_server($current_server);
                        return $cv->send($current_server);
                    }
                }

                delete $servers{$server};
                my $error = $@ || "No servers returned from $server\n";
                if ( ref $error && $error->{status_msg} ) {
                    $error = $error->{status_msg} . " from $server\n";
                }
                print STDERR $error;

                return if %servers;
                $cv->croak(
                    $cv->es->build_error(
                        'NoServers',
                        "Could not retrieve a list of active servers: $@",
                        { servers => \@all_servers }
                    )
                );

            },
            1
        );
    }
    return $self;
}

=item C<es()>

=cut

#===================================
sub es { $_[0]->{es} }
#===================================

=item C<cb()>

=cut

#===================================
sub cb {
#===================================
    my $self = shift;
    return $self->SUPER::cb unless @_;
    my $cb = shift;

    my $cv = $self;
    Scalar::Util::weaken($cv) if $_[0];
    $self->SUPER::cb(
        sub {
            local $@;
            my $result = eval { $cv->recv };
            $cb->($result);
        }
    );
}

=item C<DESTROY()>

=cut

#===================================
sub DESTROY {
#===================================
    my $self = shift;
    if ( $self->{servers} ) {
        %{ $self->{servers} } = ();
    }
    use Data::Dump qw(pp);
    $destroyed++;
    pp( {   DESTROYING =>
                { created => $count, destroyed => $destroyed, self => $self }
        }
    );
}

#===================================
#===================================
package ElasticSearch::AnyEvent::Error;
#===================================
#===================================
our @ISA = qw(ElasticSearch::Error);
@ElasticSearch::AnyEvent::Error::Internal::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::Internal' );
@ElasticSearch::AnyEvent::Error::Param::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::Param', );
@ElasticSearch::AnyEvent::Error::NoServers::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::NoServers' );
@ElasticSearch::AnyEvent::Error::Request::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::Request' );
@ElasticSearch::AnyEvent::Error::Connection::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::Connection' );
@ElasticSearch::AnyEvent::Error::Timeout::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::Timeout' );
@ElasticSearch::AnyEvent::Error::JSON::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::JSON' );
1
