package ElasticSearch::AnyEvent;

use strict;
use warnings FATAL => 'all';
use base 'ElasticSearch';
use Scalar::Util();
use AnyEvent::HTTP qw(http_request);

#===================================
sub multi_task {
#===================================
    my ( $self, $params ) = ElasticSearch::_params(@_);

    my $action = $params->{action}
        or $self->throw( 'Param', "Missing required param 'action'" );

    $self->throw( 'Param', "Unknown action '$action'" )
        unless $self->can($action);

    my $max       = $params->{max}       || 10;
    my $min_queue = $params->{min_queue} || $max * 2;

    my @queue      = ( @{ $params->{queue} || [] } );
    my $pull_queue = $params->{pull_queue};
    my $on_success = $params->{on_success};
    my $on_error
        = exists $params->{on_error}
        ? $params->{on_error}
        : sub { warn $_[0] };

    my ( $queue_empty, $running, $errors, $group_ended ) = (0) x 4;
    $queue_empty = 1 unless $pull_queue;

    my ( $issue_job, $croak_cb, %jobs );

    my $group_cv = $self->cv( on_destroy => sub { %jobs = () } );

    my $weak_group_cv = $group_cv;
    Scalar::Util::weaken($weak_group_cv);
    $group_cv->guard($group_cv) unless defined wantarray;

    $group_cv->begin(
        sub {
            my $cv = shift;
            $cv->send( $errors == 0 );
            $cv->clear_guard;
        }
    );

    $croak_cb = sub {
        %jobs = ();
        $weak_group_cv->croak(@_);
        $weak_group_cv->clear_guard if $weak_group_cv;
        0;
    };

    $issue_job = sub {
        return unless $running < $max;
        if ( !$queue_empty and @queue < $min_queue ) {
            my @new_args;
            eval {
                @new_args = $pull_queue->();
                1;
            } or return $croak_cb->($@);
            $queue_empty = 1 unless @new_args;
            push @queue, @new_args;
        }
        unless (@queue) {
            $group_ended++ || $weak_group_cv->end;
            return;
        }

        my $args = shift @queue;
        my $job_cv = eval { $self->$action($args) } or return $croak_cb->($@);
        $jobs{"$job_cv"} = $job_cv;
        Scalar::Util::weaken($job_cv);
        $running++;
        $weak_group_cv->begin;
        $job_cv->cb(
            sub {
                $running--;
                $weak_group_cv->end;
                delete $jobs{"$job_cv"};

                my $error = $@;
                eval {
                    if ( my $result = shift )
                    {
                        $on_success->( $args, $result ) if $on_success;
                    }
                    else {
                        $errors++;
                        $on_error->( $error, $args, \@queue ) if $on_error;
                    }
                    1;
                } or return $croak_cb->($@);
                $issue_job->();
            },
            'weak'
        );

        return 1;
    };
    while (1) { $issue_job->() || last }
    return $group_cv

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

    my $cv = $self->cv;

    my $cv_weak = $cv;
    Scalar::Util::weaken $cv_weak;

    $cv->guard($cv) unless defined wantarray();
    my ( $request_cb, $server_cb, $server );

    $server_cb = sub {
        $server = shift;
        unless ($server) {
            $cv_weak->croak($@);
            return;
        }
        $cv_weak->guard(
            http_request(
                $method => $server . $cmd,
                body    => $data,
                timeout => $self->timeout,
                $request_cb
            )
        );
    };

    $request_cb = sub {
        $self->_log_request( $method, $server, $cmd, $data );
        my ( $result, $error ) = $self->_parse_response( $server, @_ );
        if ($result) {
            $cv_weak->send($result);
            $cv_weak->clear_guard if $cv_weak;
            return;
        }

        if ( ref $error
            && $error->isa('ElasticSearch::AnyEvent::Error::Connection') )
        {
            my $local_cv = $cv_weak;
            $cv_weak->clear_guard;
            $cv_weak->guard( $self->refresh_servers->cb($server_cb) );
            return;
        }
        $cv_weak->croak($error);
        $cv_weak->clear_guard if $cv_weak;
    };

    my $request = $self->current_server;
    $cv->guard($request);
    $request->cb( $server_cb, 'weak' );
    return $cv;

}

#===================================
sub current_server {
#===================================
    my $self = shift;
    if (@_) {
        $self->_set_current_server(@_);
    }

    my $cv = $self->cv;
    if ( my $current_server = $self->_get_current_server ) {
        $cv->send($current_server);
        return $cv;
    }

    my $refresh_cv = $self->refresh_servers;
    $cv->guard($refresh_cv);
    $cv->guard($cv) unless defined wantarray;

    my $cv_weak = $cv;
    Scalar::Util::weaken($cv_weak);

    $refresh_cv->cb(
        sub {
            $cv_weak->clear_guard;
            if ( my $current_server = shift ) {
                return $cv_weak->send($current_server);
            }
            return $cv_weak->croak($@);
        },
        'weak'
    );

    return $cv;
}

#===================================
sub refresh_servers {
#===================================
    my $self = shift;
    $self->_set_current_server(undef);

    my %servers = map { $_ => 1 }
        ( @{ $self->servers }, @{ $self->default_servers } );
    my @all_servers = keys %servers;

    my $cv = $self->cv( on_destroy => sub { %servers = () } );

    my $cv_weak = $cv;
    Scalar::Util::weaken($cv_weak);
    $cv->guard($cv) unless defined wantarray;

    foreach my $server ( keys %servers ) {
        $servers{$server} = http_request(
            GET     => $server . '/_cluster/nodes',
            timeout => $self->timeout,
            sub {
                if ( my $current = $self->_get_current_server ) {
                    %servers = ();
                    $cv_weak->send($current);
                    $cv_weak->clear_guard if $cv_weak;
                    return;
                }
                delete $servers{$server};

                $self->_log_request( 'GET', $server, '/_cluster/nodes' );
                my ( $result, $error )
                    = $self->_parse_response( $server, @_ );

                if ($result) {
                    if ( my $current_server = $self->_parse_nodes($result) ) {
                        %servers = ();
                        $cv_weak->send($current_server);
                        $cv_weak->clear_guard if $cv_weak;
                        return;
                    }
                    $self->throw( 'Internal',
                        "Couldn't extract servers from ", $result );
                }

                return if %servers;

                $cv_weak->croak(
                    $self->build_error(
                        'NoServers',
                        "Could not retrieve a list of active servers: ",
                        { servers => \@all_servers }
                    )
                );

                $cv_weak->clear_guard if $cv_weak;

            }
        );
    }

    return $cv;
}

our %Connection_Errors = map { $_ => 1 }
    ( 'Connection reset by peer', 'Connection refused', 'Broken pipe' );

#===================================
sub _parse_response {
#===================================
    my ( $self, $server, $content, $headers ) = @_;

    my ( $result, $json_error );
    eval { $result = $self->JSON->decode($content); 1 }
        or $json_error = ( $@ || 'Unknown JSON error' );

    $self->_log_result( $result || $content );

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

    my $error = $self->build_error( $error_type, $error_msg, $error_params );
    return ( undef, $error );
}

=item C<cv()>

=cut

#===================================
sub cv { shift; ElasticSearch::AnyEvent::CondVar->new(@_) }
#===================================

#===================================
#===================================
package ElasticSearch::AnyEvent::CondVar;
#===================================
#===================================

use AnyEvent();
our @ISA = qw(AnyEvent::CondVar);

my $created = my $destroyed = 0;

#===================================
sub new {
#===================================
    my $proto = shift;
    my $class = ref $proto || $proto;
    my $self  = AnyEvent->condvar;
    bless $self, $class;
    my %params = @_;
    $created++;
    $self->{$_} = $params{$_} for keys %params;
    $self->{_guard} = [];
    return $self;
}

=item C<guard()>

=cut

#===================================
sub guard {
#===================================
    my $self = shift;
    push @{ $self->{_guard} }, @_;
}

=item C<clear_guard()>

=cut

#===================================
sub clear_guard { shift->{_guard} = []; }
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
    $destroyed++;
    ( delete $self->{on_destroy} )->() if $self->{on_destroy};
}

#===================================
sub stats {
#===================================
    print "Created: $created\nDestroyed: $destroyed\n";
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
