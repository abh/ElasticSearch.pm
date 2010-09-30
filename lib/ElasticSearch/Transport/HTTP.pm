package ElasticSearch::Transport::HTTP;

use strict;
use warnings FATAL => 'all';
use LWP::UserAgent();
use LWP::ConnCache();
use HTTP::Request();
use Encode qw(decode_utf8);

use parent 'ElasticSearch::Transport::Base';

#===================================
sub _ua {
#===================================
    my $self = shift;
    unless ( $self->{_ua}{$$} ) {
        $self->{_ua} = {
            $$ => LWP::UserAgent->new(
                timeout    => $self->es->timeout,
                conn_cache => LWP::ConnCache->new
            )
        };

    }
    return $self->{_ua}{$$};
}

#===================================
sub request {
#===================================
    my $self           = shift;
    my $current_server = shift;
    my $method         = shift;
    my $cmd            = shift;
    my $data           = shift;

    my $es = $self->es;

    my $request = HTTP::Request->new( $method, $current_server . $cmd );

    $request->add_content_utf8($data)
        if defined $data;

    $es->_log_request( $method, $current_server, $cmd, $data );

    my $server_response = $self->_ua->request($request);

    my $content = $server_response->decoded_content;
    $content = decode_utf8($content);

    my ( $result, $json_error );
    eval { $result = $es->JSON->decode($content); 1 }
        or $json_error = ( $@ || 'Unknown JSON error' );

    $es->_log_result( $result || $content );

    my $success = $server_response->is_success;

    return $result if $success && !$json_error;

    my $error_params = {
        server      => $current_server,
        response    => $json_error ? $content : $result,
        status_code => $server_response->code,
        status_msg  => $server_response->message,

    };

    my ( $error_type, $error_msg );
    if ( $success && $json_error ) {
        $error_type = 'JSON';
        $error_msg  = $json_error;
    }
    else {
        $error_msg = $error_params->{status_msg};
        $error_type
            = $error_msg eq 'read timeout' ? 'Timeout'
            : $error_msg =~ /Can't connect|Server closed connection/
            ? 'Connection'
            : 'Request';
        $error_msg
            = $result && $result->{error}
            ? $result->{error}
            : $error_params->{status_msg} . ' ('
            . $error_params->{status_code} . ')';
    }

    $es->throw( $error_type, $error_msg, $error_params );
}

1;
