package ElasticSearch::AnyEvent;

use strict;
use warnings FATAL => 'all';
use AE();
use AnyEvent::HTTP qw(http_request);

use base 'ElasticSearch';

#===================================
sub refresh_servers {
#===================================
    my $self = shift;
    my @servers = ( @{ $self->servers }, @{ $self->{_default_servers} } );

    my @live_servers;
    foreach my $server (@servers) {
        next unless $server;
        $server = 'http://' . $server
            unless $server =~ m{^http(?:s)?://};
        $server =~ s{/+$}{};

        my $nodes = eval {
            my $result
                = $self->_request( $server, 'GET', '/_cluster/nodes' )
                ->result;
            return $result->{nodes};
        } or next;

        @live_servers = grep {$_}
            map { $_->{http_address} || $_->{httpAddress} } values %$nodes;
        last if @live_servers;
    }

    $self->throw(
        'NoServers',
        "Could not retrieve a list of active servers: $@",
        { servers => \@servers }
    ) unless @live_servers;
    for (@live_servers) {
        $_ =~ m{inet\[(\S*)/(\S+):(\d+)\]};
        $_ = 'http://' . ( $1 || $2 ) . ':' . $3;
    }

    $self->{_servers} = { $$ => \@live_servers };
    $self->{_current_server}
        = { $$ => $live_servers[ int( rand(@live_servers) ) ] };
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

    my $current_server = $self->current_server;
    return $self->_request( $current_server, $method, $cmd, $data );
}

#===================================
sub _request {
#===================================
    my ( $self, $server, $method, $cmd, $data, $result ) = @_;
    my $allow_refresh;
    unless ($server) {
        $allow_refresh = 1;
        $server        = $self->current_server;
    }

    $result ||= ElasticSearch::AnyEvent::Result->new(@_);
    $self->_log_request( $method, $server, $cmd, $data );

    http_request(
        $method => $server . $cmd,
        body    => $data,
        timeout => 2,
        sub {
            my $conn_err = $result->parse_response( $server, @_ ) or return;
            return unless $allow_refresh;

            warn "Error connecting to '$server' : $conn_err";
            $self->refresh_servers;
            $self->_request( @_, $result );
        }
    );

    return $result;
}

#===================================
#===================================
package ElasticSearch::AnyEvent::Result;
#===================================
#===================================

#===================================
sub new {
#===================================
    my $proto = shift;
    my $class = ref $proto || $proto;
    my $es    = shift;
    bless { cv => AE::cv(), es => $es, args => \@_ }, $class;
}

=item C<parse_response()>

=cut

#===================================
sub parse_response {
#===================================
    my ( $self, $server, $content, $headers ) = @_;

    my ( $status, $reason ) = @{$headers}{qw(Status Reason)};
    return $reason
        if $status eq '599'
            && (   $reason eq 'Connection reset by peer'
                || $reason eq 'Connection refused'
                || $reason eq 'Broken pipe'
                || $reason eq 'Connection timed out' && !$self->{args}->[0] );

    $content = '' unless defined $content;

    my $es = $self->es;

    my ( $result, $json_error );
    eval { $result = $es->JSON->decode($content); 1 }
        or $json_error = ( $@ || 'Unknown JSON error' );

    $es->_log_result( $result || $content );

    my $success = $status =~ /^2/;

    if ( $success && !$json_error ) {
        delete $self->{error};
        $self->cv->send($result);
        return;
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
            = $reason eq 'Connection timed out'
            ? 'Timeout'
            : 'Request';
        $error_msg
            = $result && $result->{error}
            ? $result->{error}
            : $error_params->{status_msg} . ' ('
            . $error_params->{status_code} . ')';
    }

    $self->{error} = [ $error_type, $error_msg, $error_params ];
    $self->cv->croak;
    return;
}

=item C<es()>

=cut

#===================================
sub es { $_[0]->{es} }
sub cv { $_[0]->{cv} }
#===================================

=item C<result()>

=cut

#===================================
sub result {
#===================================
    my $self   = shift;
    my $result = $self->cv->recv;
    return $result if $result;
    $self->es->throw( @{ $self->{error} } )

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
@ElasticSearch::AnyEvent::Error::Timeout::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::Timeout' );
@ElasticSearch::AnyEvent::Error::JSON::ISA
    = ( __PACKAGE__, 'ElasticSearch::Error::JSON' );
1
