package ElasticSearch;

use strict;
use warnings FATAL => 'all';
use LWP::UserAgent();
use LWP::ConnCache();
use HTTP::Request();
use JSON::XS();
use Encode qw(decode_utf8);

our $VERSION = '0.13';

use constant {
    ONE_REQ     => 1,
    ONE_OPT     => 2,
    MULTI_ALL   => 3,
    MULTI_BLANK => 4,
};

use constant {
    CMD_INDEX_TYPE_ID => [ index => ONE_REQ, type => ONE_REQ, id => ONE_REQ ],
    CMD_INDEX_TYPE_id => [ index => ONE_REQ, type => ONE_REQ, id => ONE_OPT ],
    CMD_index      => [ index => MULTI_BLANK ],
    CMD_INDEX      => [ index => ONE_REQ ],
    CMD_INDEX_type => [ index => ONE_REQ, type => MULTI_BLANK ],
    CMD_index_TYPE => [ index => MULTI_ALL, type => ONE_REQ ],
    CMD_index_type => [ index => MULTI_ALL, type => MULTI_BLANK ],
    CMD_nodes      => [ node  => MULTI_BLANK ],
};

our %QS_Format = (
    boolean  => '1 | 0',
    duration => "'5m' | '10s'",
    fixed    => '',
    optional => "'scalar value'",
    flatten  => "'scalar' or ['scalar_1', 'scalar_n']",
    'int'    => "integer",
    string   => '"string"',
    float    => 'float',
    enum     => '"predefined_value"',
);

our %QS_Formatter = (
    fixed   => sub { return $_[1] },
    boolean => sub { return $_[0] ? $_[1] : $_[2] },
    duration => sub {
        my ( $t, $k ) = @_;
        return unless defined $t;
        return "$k=$t" if $t =~ /^\d+[smh]$/i;
        die "$k '$t' is not in the form $QS_Format{duration}\n";
    },
    flatten => sub {
        my $array = shift or return;
        my $key = shift;
        return "$key=" . ( ref $array ? join( ',', @$array ) : $array );
    },
    'int' => sub {
        my $int = shift;
        return unless defined $int;
        my $key = shift;
        eval { $int += 0; 1 } or die "'$key' is not an integer";
        return $key . '=' . $int;
    },
    'float' => sub {
        my $float = shift;
        return unless defined $float;
        my $key = shift;
        eval { $float += 0; 1 } or die "'$key' is not a float";
        return $key . '=' . $float;
    },
    'string' => sub {
        my $string = shift;
        return unless defined $string;
        return shift() . '=' . $string;
    },
    'enum' => sub {
        my $val = shift;
        return unless defined $val;
        my $key  = shift;
        my $vals = $_[0];
        for (@$vals) {
            return "${key}=$val" if $val eq $_;
        }
        die "Unrecognised value '$val'. Allowed values: "
            . join( ', ', @$vals );
    },

);

#===================================
sub new {
#===================================
    my ( $proto, $params ) = &_params;
    my $self = bless { _JSON => JSON::XS->new(), _base_qs => [], _camel_case=>0 },
        ref $proto || $proto;
    my $servers = delete $params->{servers};
    for ( keys %$params ) {
        $self->$_( $params->{$_} );
    }
    $self->{_servers}{$$} = ref $servers eq 'ARRAY' ? $servers : [$servers];
    return $self;
}

#===================================
sub get { shift()->_do_action( 'get', { cmd => CMD_INDEX_TYPE_ID }, @_ ) }
#===================================

my %Index_Defn = (
    cmd => CMD_INDEX_TYPE_id,
    qs  => {
        create  => [ 'boolean',  'op_type=create' ],
        timeout => [ 'duration', 'timeout' ],
    },
    data => 'data',
);

#===================================
sub index {
#===================================
    my ( $self, $params ) = &_params;
    $self->_index( 'index', \%Index_Defn, $params );
}

#===================================
sub set {
#===================================
    my ( $self, $params ) = &_params;
    $self->_index( 'set', \%Index_Defn, $params );
}

#===================================
sub create {
#===================================
    my ( $self, $params ) = &_params;
    $self->_index( 'create', { %Index_Defn, postfix => '_create' }, $params );
}

#===================================
sub _index {
#===================================
    my $self = shift;
    $_[1]->{method} = $_[2]->{id} ? 'PUT' : 'POST';
    $self->_do_action(@_);
}

#===================================
sub delete {
#===================================
    shift()->_do_action(
        'delete',
        {   method => 'DELETE',
            cmd    => CMD_INDEX_TYPE_ID,
        },
        @_
    );
}

my %Search_Data = (
    facets        => ['facets'],
    from          => ['from'],
    size          => ['size'],
    explain       => ['explain'],
    fields        => ['fields'],
    'sort'        => ['sort'],
    highlight     => ['highlight'],
    indices_boost => ['indices_boost'],
);

my %Search_Defn = (
    cmd     => CMD_index_type,
    postfix => '_search',
    qs      => {
        search_type => [
            'enum',
            'search_type',
            [   qw(  dfs_query_then_fetch     dfs_query_and_fetch
                    query_then_fetch         query_and_fetch)
            ]
        ],
        scroll => [ 'duration', 'scroll' ],
    },
    data => { %Search_Data, query => ['query'] }
);

my %Query_Defn = (
    bool           => ['bool'],
    constant_score => ['constant_score'],
    dis_max        => ['dis_max'],
    field          => ['field'],
    filtered       => ['filtered'],
    flt            => [ 'flt', 'fuzzy_like_this' ],
    flt_field      => [ 'flt_field', 'fuzzy_like_this_field' ],
    match_all      => ['match_all'],
    mlt            => [ 'mlt', 'more_like_this' ],
    mlt_field      => [ 'mlt_field', 'more_like_this_field' ],
    prefix         => ['prefix'],
    query_string   => ['query_string'],
    range          => ['range'],
    term           => ['term'],
    wildcard       => ['wildcard'],
);

#===================================
sub search { shift()->_do_action( 'search', \%Search_Defn, @_ ) }
#===================================

#===================================
sub scroll {
#===================================
    shift()->_do_action(
        'scroll',
        {   cmd    => [],
            prefix => '_search/scroll',
            qs     => { scroll_id => [ 'string', 'scroll_id' ] }
        },
        @_
    );
}

#===================================
sub delete_by_query {
#===================================
    shift()->_do_action(
        'delete_by_query',
        {   %Search_Defn,
            method  => 'DELETE',
            postfix => '_query',
            data    => \%Query_Defn,
        },
        @_
    );
}

#===================================
sub count {
#===================================
    shift()->_do_action(
        'count',
        {   %Search_Defn,
            postfix => '_count',
            data    => \%Query_Defn,
        },
        @_
    );
}

#===================================
sub terms {
#===================================
    shift()->_do_action(
        'terms',
        {   cmd     => CMD_index,
            postfix => '_terms',
            qs      => {
                'fields'   => [ 'flatten', 'fields' ],
                'gt'       => [ 'string',  'gt' ],
                'lt'       => [ 'string',  'lt' ],
                'gte'      => [ 'string',  'gte' ],
                'lte'      => [ 'string',  'lte' ],
                'prefix'   => [ 'string',  'prefix' ],
                'regexp'   => [ 'string',  'regexp' ],
                'min_freq' => [ 'int',     'min_freq' ],
                'max_freq' => [ 'int',     'max_freq' ],
                'size'     => [ 'int',     'size' ],
                'sort'     => [ 'enum',    'sort', [qw(term freq)] ],
            }
        },
        @_
    );
}

#===================================
sub mlt {
#===================================
    shift()->_do_action(
        'mlt',
        {   cmd    => CMD_INDEX_TYPE_ID,
            method => 'GET',
            qs     => {
                %{ $Search_Defn{qs} },
                mlt_fields => [ 'flatten', 'mlt_fields' ],
                pct_terms_to_match => [ 'float', 'percent_terms_to_match ' ],
                min_term_freq      => [ 'int',   'min_term_freq' ],
                max_query_terms    => [ 'int',   'max_query_terms' ],
                stop_words   => [ 'flatten', 'stop_words' ],
                min_doc_freq => [ 'int',     'min_doc_freq' ],
                max_doc_freq => [ 'int',     'max_doc_freq' ],
                min_word_len => [ 'int',     'min_word_len' ],
                max_word_len => [ 'int',     'max_word_len' ],
                boost_terms  => [ 'float',   'boost_terms' ],
            },
            data    => 'data',
            postfix => '_mlt',
            data    => \%Search_Data,
        },
        @_
    );
}

#===================================
sub index_status {
#===================================
    shift()->_do_action(
        'index_status',
        {   cmd     => CMD_index,
            postfix => '_status'
        },
        @_
    );
}

#===================================
sub create_index {
#===================================
    shift()->_do_action(
        'create_index',
        {   method  => 'PUT',
            cmd     => CMD_INDEX,
            postfix => '',
            data    => { index => ['defn'] },
        },
        @_
    );
}

#===================================
sub delete_index {
#===================================
    shift()->_do_action(
        'delete_index',
        {   method  => 'DELETE',
            cmd     => CMD_INDEX,
            postfix => ''
        },
        @_
    );
}

#===================================
sub clear_cache {
#===================================
    shift()->_do_action(
        'clear_cache',
        {   method  => 'POST',
            cmd     => CMD_index,
            postfix => '_cache/clear'
        },
        @_
    );
}

#===================================
sub aliases {
#===================================
    my $self    = shift;
    my $params  = $self->_params(@_);
    my $actions = $params->{actions};
    if ( defined $actions && ref $actions ne 'ARRAY' ) {
        $params->{actions} = [$actions];
    }

    $self->_do_action(
        'aliases',
        {   prefix => '_aliases',
            method => 'POST',
            cmd    => [],
            data   => { actions => 'actions' },
        },
        $params
    );
}

#===================================
sub get_aliases {
#===================================
    my ( $self, $params ) = &_params;
    my $results = $self->index_status($params);
    my $indices = $results->{indices};
    my %aliases = ( indices => {}, aliases => {} );
    foreach my $index ( keys %$indices ) {
        my $aliases = $indices->{$index}{aliases};
        $aliases{indices}{$index} = $aliases;
        for (@$aliases) {
            push @{ $aliases{aliases}{$_} }, $index;
        }

    }
    return \%aliases;
}

#===================================
sub flush_index {
#===================================
    shift()->_do_action(
        'flush_index',
        {   method  => 'POST',
            cmd     => CMD_index,
            postfix => '_flush',
            qs      => { refresh => [ 'boolean', 'refresh=true' ] },
        },
        @_
    );
}

#===================================
sub refresh_index {
#===================================
    shift()->_do_action(
        'refresh_index',
        {   method  => 'POST',
            cmd     => CMD_index,
            postfix => '_refresh'
        },
        @_
    );
}
#===================================
sub optimize_index {
#===================================
    shift()->_do_action(
        'optimize_index',
        {   method  => 'POST',
            cmd     => CMD_index,
            postfix => '_optimize',
            qs      => {
                only_deletes => [ 'boolean', 'only_expunge_deletes=true' ],
                refresh      => [ 'boolean', 'refresh=true' ],
                flush        => [ 'boolean', 'flush=true' ]
            },
        },
        @_
    );
}

#===================================
sub snapshot_index {
#===================================
    shift()->_do_action(
        'snapshot_index',
        {   method  => 'POST',
            cmd     => CMD_index,
            postfix => '_gateway/snapshot'
        },
        @_
    );
}

#===================================
sub gateway_snapshot {
#===================================
    shift()->_do_action(
        'gateway_snapshot',
        {   method  => 'POST',
            cmd     => CMD_index,
            postfix => '_gateway/snapshot'
        },
        @_
    );
}

#===================================
sub put_mapping {
#===================================
    my ( $self, $params ) = &_params;
    $self->_do_action(
        'put_mapping',
        {   method  => 'PUT',
            cmd     => CMD_index_TYPE,
            postfix => '_mapping',
            qs      => {
                timeout          => [ 'duration', 'timeout' ],
                ignore_conflicts => [ 'boolean',  'ignore_conflicts=true' ]
            },
            data => {
                properties => 'properties',
                _all       => ['_all']
            }
        },
        $params
    );
}

#===================================
sub get_mapping {
#===================================
    my ( $self,  $params ) = &_params;
    my ( $index, $type )   = @{$params}{qw(index type)};

    my $error
        = ref $index ? "'index' must be a single value\n"
        : defined $index && length $index ? ''
        :   "Parameter 'index' is a required value\n";

    $self->throw(
        'Param',
        $error . $self->_usage( 'get_mapping', { cmd => CMD_INDEX_type } ),
        { params => $params }
    ) if $error;

    my $state = $self->cluster_state;

    my $source = $state->{metadata}{indices}{$index}{mappings}
        or return;
    my $json = $self->JSON;
    my @types
        = !$type ? keys %$source
        : ref $type eq 'ARRAY' ? @$type
        :                        $type;

    my %mappings;
    for (@types) {
        my $val = $source->{$_}{source} or next;
        my ( $key, $mapping ) = %{ $json->decode($val) };
        $mappings{$key} = $mapping;
    }
    return $type && !ref $type
        ? $mappings{$type}
        : \%mappings;

}

#===================================
sub cluster_state {
#===================================
    shift()
        ->_do_action( 'cluster_state', { prefix => '_cluster/state' }, @_ );
}

#===================================
sub nodes {
#===================================
    shift()->_do_action(
        'nodes',
        {   prefix => '_cluster/nodes',
            cmd    => CMD_nodes,
            qs     => { settings => [ 'boolean', 'settings=true' ] }
        },
        @_
    );
}

#===================================
sub nodes_stats {
#===================================
    shift()->_do_action(
        'nodes',
        {   prefix  => '_cluster/nodes',
            postfix => 'stats',
            cmd     => CMD_nodes,
        },
        @_
    );
}

#===================================
sub shutdown {
#===================================
    shift()->_do_action(
        'shutdown',
        {   method  => 'POST',
            prefix  => '_cluster/nodes',
            cmd     => CMD_nodes,
            postfix => '_shutdown',
            qs      => { delay => [ 'duration', 'delay' ] }
        },
        @_
    );
}

#===================================
sub restart {
#===================================
    shift()->_do_action(
        'shutdown',
        {   method  => 'POST',
            prefix  => '_cluster/nodes',
            cmd     => CMD_nodes,
            postfix => '_restart',
            qs      => { delay => [ 'duration', 'delay' ] }
        },
        @_
    );
}

#===================================
sub cluster_health {
#===================================
    shift()->_do_action(
        'cluster_health',
        {   prefix => '_cluster/health',
            cmd    => CMD_index,
            qs     => {
                level => [ 'enum', 'level', [qw(cluster indices shards)] ],
                wait_for_status =>
                    [ 'enum', 'wait_for_status', [qw(green yellow red)] ],
                wait_for_relocating_shards =>
                    [ 'int', 'wait_for_relocating_shards' ],
                timeout => [ 'duration', 'timeout' ]
            }
        },
        @_
    );
}

#===================================
sub camel_case {
#===================================
    my $self = shift;
    if (@_) {
        if ( shift() ) {
            $self->{_camel_case} = 1;
            $self->{_base_qs}    = ['case=camelCase'];
        }
        else {
            $self->{_camel_case} = 0;
            $self->{_base_qs}    = [];
        }
    }
    return $self->{_camel_case};
}

#===================================
sub _do_action {
#===================================
    my $self            = shift;
    my $action          = shift || '';
    my $defn            = shift || {};
    my $original_params = $self->_params(@_);

    my ( $cmd, $data, $error );

    my $params = {%$original_params};
    eval {
        $cmd = join '',
            grep {defined}
            $self->_build_cmd( $params, @{$defn}{qw(prefix cmd postfix)} ),
            $self->_build_qs( $params, $defn->{qs} );

        $data = $self->_build_data( $params, $defn->{data} );
        die "Unknown parameters: " . join( ', ', keys %$params ) . "\n"
            if keys %$params;
        1;
    } or $error = $@ || 'Unknown error';

    $self->throw(
        'Param',
        $error . $self->_usage( $action, $defn ),
        { params => $original_params }
    ) if $error;

    return $self->request( {
            method => $defn->{method} || 'GET',
            cmd    => $cmd,
            data   => $data,
        }
    );
}

#===================================
sub _usage {
#===================================
    my $self   = shift;
    my $action = shift;
    my $defn   = shift;

    my $usage = "Usage for '$action()':\n";
    my @cmd = @{ $defn->{cmd} || [] };
    while ( my $key = shift @cmd ) {
        my $type = shift @cmd;
        my $arg_format
            = $type == ONE_REQ ? "\$$key"
            : $type == ONE_OPT ? "\$$key"
            :                    "\$$key | [\$${key}_1,\$${key}_n]";

        my $required = $type == ONE_REQ ? 'required' : 'optional';
        $usage .= sprintf( "  - %-20s =>  %-45s # %s\n",
            $key, $arg_format, $required );
    }
    if ( my $qs = $defn->{qs} ) {
        for ( sort keys %$qs ) {
            my $arg_format = $QS_Format{ $qs->{$_}[0] };
            $usage .= sprintf( "  - %-20s =>  %-45s # optional\n", $_,
                $arg_format );
        }
    }

    if ( my $data = $defn->{data} ) {
        $data = { data => $data } unless ref $data eq 'HASH';
        my @keys = sort { $a->[0] cmp $b->[0] }
            map { ref $_ ? [ $_->[0], 'optional' ] : [ $_, 'required' ] }
            values %$data;

        for (@keys) {
            $usage .= sprintf(
                "  - %-20s =>  %-45s # %s\n",
                $_->[0], '{' . $_->[0] . '}',
                $_->[1]
            );
        }
    }
    return $usage;
}

#===================================
sub _build_qs {
#===================================
    my $self   = shift;
    my $params = shift;
    my $defn   = shift or return;
    my @qs     = ( @{ $self->{_base_qs} } );
    foreach my $key ( keys %$defn ) {
        my ( $format_name, @args ) = @{ $defn->{$key} || [] };
        $format_name ||= '';

        next unless exists $params->{$key} || $format_name eq 'fixed';

        my $formatter = $QS_Formatter{$format_name}
            or die "Unknown QS formatter '$format_name'";

        my $val = $formatter->( delete $params->{$key}, @args );
        push @qs, $val if defined $val;
    }
    return unless @qs;
    return '?' . join '&', @qs;
}

#===================================
sub _build_data {
#===================================
    my $self   = shift;
    my $params = shift;
    my $defn   = shift or return;

    my $top_level;
    if ( ref $defn ne 'HASH' ) {
        $top_level = 1;
        $defn = { data => $defn };
    }

    my %data;
KEY: while ( my ( $key, $source ) = each %$defn ) {
        if ( ref $source eq 'ARRAY' ) {
            foreach (@$source) {
                my $val = delete $params->{$_};
                next unless defined $val;
                $data{$key} = $val;
                next KEY;
            }
        }
        else {
            $data{$key} = delete $params->{$source}
                or die "Missing required param '$source'\n";
        }
    }
    if ($top_level) {
        die "Param '$defn' is not a HASH ref"
            unless ref $data{data} eq 'HASH';
        return $data{data};
    }
    return \%data;
}

#===================================
sub _build_cmd {
#===================================
    my $self   = shift;
    my $params = shift;
    my ( $prefix, $defn, $postfix ) = @_;

    my @defn = ( @{ $defn || [] } );
    my @cmd;
    while (@defn) {
        my $key  = shift @defn;
        my $type = shift @defn;

        my $val = delete $params->{$key};
        if ( defined $val ) {
            if ( ref $val eq 'ARRAY' ) {
                die "'$key' must be a single value\n"
                    if $type == ONE_REQ || $type == ONE_OPT;
                $val = join ',', @$val;
            }
        }
        else {
            next if $type == ONE_OPT || $type == MULTI_BLANK;
            die "Param '$key' is required\n"
                if $type == ONE_REQ;
            $val = '_all';
        }
        push @cmd, $val;
    }

    return join '/', '', grep {defined} ( $prefix, @cmd, $postfix );
}

#===================================
sub servers { shift->{_servers}{$$} }
#===================================

#===================================
sub refresh_servers {
#===================================
    my $self = shift;
    my @servers
        = @_ == 0 ? @{ ( values %{ $self->{_servers} } )[0] || [] }
        : ref $_[0] eq 'ARRAY' ? ( @{ $_[0] } )
        :                        (@_);

    my @live_servers;
    foreach my $server (@servers) {
        next unless $server;
        $server = 'http://' . $server
            unless $server =~ m{^http(?:s)?://};
        $server =~ s{/+$}{};

        my $nodes = eval {
            my $result = $self->_request( $server, 'GET', '/_cluster/nodes' );
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
sub current_server {
#===================================
    my $self = shift;
    unless ( $self->{_current_server}{$$} ) {
        $self->refresh_servers;
    }
    return $self->{_current_server}{$$};
}

#===================================
sub ua {
#===================================
    my $self = shift;
    unless ( $self->{_ua}{$$} ) {
        my $ua = $self->{_ua} = LWP::UserAgent->new( %{ $self->ua_options } );
        $ua->conn_cache( LWP::ConnCache->new );
        $self->{_ua} = { $$ => $ua };
    }
    return $self->{_ua}{$$};
}

#===================================
sub ua_options {
#===================================
    my $self = shift;
    if (@_) {
        ( undef, my $params ) = $self->_params(@_);
        $self->{_ua_options} = $params;
    }
    return $self->{_ua_options} ||= {};
}

#===================================
sub JSON { shift()->{_JSON} }
#===================================

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
    $data = $json->encode($data)
        if defined $data;

    while (1) {
        my $current_server = $self->current_server;
        my ( $result, $error );
        eval {
            $result
                = $self->_request( $current_server, $method, $cmd, $data );
            1;
        } or $error = $@ || 'Unknown error';

        return $result unless $error;

        if ( ref $error ) {
            my $code = $error->{-vars}{status_code} || 0;
            my $msg  = $error->{-vars}{status_msg}  || '';
            if ( $code == 500 && $msg =~ /Can't connect/ ) {
                warn "Error connecting to '$current_server' : $msg";
                $self->refresh_servers;
                next;
            }
            $error->{-vars}{request} = $params;
            die $error;
        }

        $self->throw( 'Request', $error, { request => $params } );
    }
}

#===================================
sub _request {
#===================================
    my $self           = shift;
    my $current_server = shift;
    my $method         = shift;
    my $cmd            = shift;
    my $data           = shift;

    my $request = HTTP::Request->new( $method, $current_server . $cmd );

    $request->add_content_utf8($data)
        if defined $data;

    $self->_log_request( $method, $current_server, $cmd, $data );

    my $server_response = $self->ua->request($request);

    my $content = $server_response->decoded_content;
    $content = decode_utf8($content);

    my ( $result, $json_error );
    eval { $result = $self->JSON->decode($content); 1 }
        or $json_error = ( $@ || 'Unknown JSON error' );

    $self->_log_result( $result || $content );

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
        $error_type = 'Request';
        $error_msg
            = $result && $result->{error}
            ? $result->{error}
            : $error_params->{status_msg} . ' ('
            . $error_params->{status_code} . ')';
    }

    $self->throw( $error_type, $error_msg, $error_params );
}

#===================================
sub _log_request {
#===================================
    my $self = shift;
    my $log = $self->trace_calls or return;
    my ( $method, $current_server, $cmd, $data ) = @_;
    if ( defined $data ) {
        $data =~ s/'/\\u0027/g;
        $data = " -d '\n${data}'";
    }
    else {
        $data = '';
    }
    print $log "curl -X${method} '${current_server}${cmd}' ${data}\n";

}

#===================================
sub _log_result {
#===================================
    my $self    = shift;
    my $log     = $self->trace_calls or return;
    my $content = shift;
    my $out     = ref $content ? $self->JSON->encode($content) : $content;
    my @lines   = split /\n/, $out;
    while (@lines) {
        my $line = shift @lines;
        if ( length $line > 65 ) {
            my ($spaces) = ( $line =~ /^(?:> )?(\s*)/ );
            $spaces = substr( $spaces, 0, 20 ) if length $spaces > 20;
            unshift @lines, '> ' . $spaces . substr( $line, 65 );
            $line = substr $line, 0, 65;
        }
        print $log "# $line\n";
    }
    print $log "\n\n";
}

#===================================
sub _params {
#===================================
    my $self = shift;
    my $params;
    if ( @_ % 2 ) {
        $self->throw(
            "Param",
            'Expecting a HASH ref or a list of key-value pairs',
            { params => \@_ }
        ) unless ref $_[0] eq 'HASH';
        $params = shift;
    }
    else {
        $params = {@_};
    }
    return ( $self, $params );
}
#===================================
sub throw {
#===================================
    my $self = shift;
    my $type = shift;
    my $msg  = shift;
    my $vars = shift;

    my $class = ref $self || $self;
    my $error_class = $class . '::Error::' . $type;

    $msg = 'Unknown error' unless defined $msg;
    $msg =~ s/\n/\n    /g;

    my $debug = ref $self ? $self->debug : 1;
    my ( undef, $file, $line ) = caller(0);
    my $error = bless {
        -text       => $msg,
        -line       => $line,
        -file       => $file,
        -vars       => $vars,
        -stacktrace => $debug ? _stack_trace() : ''
    }, $error_class;

    die $error;
}

#===================================
sub _stack_trace {
#===================================
    my $i    = 2;
    my $line = ( '-' x 60 ) . "\n";
    my $o    = $line
        . sprintf( "%-4s %-30s %-5s %s\n",
        ( '#', 'Package', 'Line', 'Sub-routine' ) )
        . $line;
    while ( my @caller = caller($i) ) {
        $o .= sprintf( "%-4d %-30s %4d  %s\n", $i++, @caller[ 0, 2, 3 ] );
        last if $caller[3] eq '(eval)';
    }
    return $o .= $line;
}

#===================================
sub debug {
#===================================
    my $self = shift;
    if (@_) { $self->{_debug} = !!shift() }
    return $self->{_debug};
}

#===================================
sub trace_calls {
#===================================
    my $self = shift;
    if (@_) {
        if ( my $file = shift ) {
            $file = '&STDERR' if $file eq "1";

            open my $fh, ">>$file"
                or $self->throw( 'Internal',
                "Couldn't open '$file' for trace logging: $!" );
            binmode( $fh, ':utf8' );
            $fh->autoflush(1);
            $self->JSON->pretty(1);
            $self->{_trace_file} = $fh;
        }
        else {
            $self->{_trace_file} = undef;
            $self->JSON->pretty(0);
        }
    }
    return $self->{_trace_file};
}

#===================================
#===================================
package ElasticSearch::Error;
#===================================
#===================================
@ElasticSearch::Error::Internal::ISA  = __PACKAGE__;
@ElasticSearch::Error::Param::ISA     = __PACKAGE__;
@ElasticSearch::Error::NoServers::ISA = __PACKAGE__;
@ElasticSearch::Error::Request::ISA   = __PACKAGE__;
@ElasticSearch::Error::JSON::ISA      = __PACKAGE__;

use strict;
use warnings FATAL => 'all', NONFATAL => 'redefine';

use overload ( '""' => 'stringify' );
use Data::Dumper;

#===================================
sub stringify {
#===================================
    my $error     = shift;
    my $object_id = $error->{-object_id};

    my $msg
        = '[ERROR] ** '
        . ( ref($error) || 'ElasticSearch::Error' ) . ' at '
        . $error->{-file}
        . ' line '
        . $error->{-line} . " : \n"
        . ( $error->{-text} || 'Missing error message' ) . "\n"
        . (
        $error->{-vars}
        ? "With vars:\n" . Dumper( $error->{-vars} ) . "\n"
        : ''
        ) . $error->{-stacktrace};
    return $msg;
}

=head1 NAME

ElasticSearch - An API for communicating with ElasticSearch

=head1 VERSION

Version 0.13, tested against ElasticSearch server version 0.7.0.

=cut

=head1 DESCRIPTION

ElasticSearch is an Open Source (Apache 2 license), distributed, RESTful
Search Engine based on Lucene, and built for the cloud, with a JSON API.

Check out its features: L<http://www.elasticsearch.com/products/elasticsearch/>

This module is a thin API which makes it easy to communicate with an
ElasticSearch cluster.

It maintains a list of all servers/nodes in the ElasticSearch cluster, and
spreads the load randomly across these nodes.  If the current active node
disappears, then it attempts to connect to another node in the list.

Forking a process triggers a server list refresh, and a new connection to
a randomly chosen node in the list.

=cut

=head1 SYNOPSIS


    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com',
        debug       => 1,
        trace_calls => 'log_file',
    );

    $e->index(
        index => 'twitter',
        type  => 'tweet',
        id    => 1,
        data  => {
            user        => 'kimchy',
            post_date   => '2009-11-15T14:12:12',
            message     => 'trying out Elastic Search'
        }
    );

    $data = $e->get(
        index => 'twitter',
        type  => 'tweet',
        id    => 1
    );

    $results = $e->search(
        index => 'twitter',
        type  => 'tweet',
        query => {
            term    => { user => 'kimchy' },
        }
    );

=cut

=head1 GETTING ElasticSearch

You can download the latest released version of ElasticSearch from
L<http://github.com/elasticsearch/elasticsearch/downloads>.

To build the latest development version from source, you can do
the following:

    cd ~
    rm -Rf elasticsearch*
    wget http://github.com/elasticsearch/elasticsearch/tarball/master
    tar -xzf elasticsearch*.gz
    cd elasticsearch*
    ./gradlew

    cd /path/where/you/want/elasticsearch
    unzip ~/elasticsearch/build/distributions/elasticsearch*

To start a test server in the foreground:

   ./bin/elasticsearch -f

You can start multiple servers by repeating this command - they will
autodiscover each other.

More instructions are available here:
L<http://www.elasticsearch.com/docs/elasticsearch/setup/installation>

=cut

=head1 CALLING CONVENTIONS

I've tried to follow the same terminology as used in the ElasticSearch docs
when naming methods, so it should be easy to tie the two together.

Some methods require a specific C<index> and a specific C<type>, while others
allow a list of indices or types, or allow you to specify all indices or
types. I distinguish between them as follows:

   $e->method( index => multi, type => single, ...)

C<multi> values can be:

      index   => 'twitter'          # specific index
      index   => ['twitter','user'] # list of indices
      index   => undef              # (or not specified) = all indices

C<single> values must be a scalar, and are required parameters

      type  => 'tweet'

=cut

=head1 RETURN VALUES AND EXCEPTIONS

Methods that query the ElasticSearch cluster return the raw data structure
that the cluster returns.  This may change in the future, but as these
data structures are still in flux, I thought it safer not to try to interpret.

Anything that is know to be an error throws an exception, eg trying to delete
a non-existent index.

=cut

=head1 METHODS

=head2 Creating a new ElasticSearch instance

=head3 C<new()>

    $e = ElasticSearch->new(
            servers     =>  '127.0.0.1:9200'            # single server
                            | ['es1.foo.com:9200',
                               'es2.foo.com:9200'],     # multiple servers
            debug       => 1 | 0,
            ua_options  => { LWP::UserAgent options},

     );

C<servers> is a required parameter and can be either a single server or an
ARRAY ref with a list of servers.  These servers are used to retrieve a list
of all servers in the cluster, after which one is chosen at random to be
the L</"current_server()">.

See also: L</"debug()">, L</"ua_options()">,
          L</"refresh_servers()">, L</"servers()">, L</"current_server()">

=cut

=head2 Document-indexing methods

=head3 C<index()>

    $result = $e->index(
        index   => single,
        type    => single,
        id      => $document_id,        # optional, otherwise auto-generated
        data    => {
            key => value,
            ...
        },
        timeout => eg '1m' or '10s'     # optional
        create  => 1 |0                 # optional
    );

eg:

    $result = $e->index(
        index   => 'twitter',
        type    => 'tweet',
        id      => 1,
        data    => {
            user        => 'kimchy',
            post_date   => '2009-11-15T14:12:12',
            message     => 'trying out Elastic Search'
        },
    );

Used to add a document to a specific C<index> as a specific C<type> with
a specific C<id>. If the C<index/type/id> combination already exists,
then that document is updated, otherwise it is created.

Note:

=over

=item *

If the C<id> is not specified, then ElasticSearch autogenerates a unique
ID and a new document is always created.

=item *

If C<create> is C<true>, then a new document is created, even if the same
C<index/type/id> combination already exists!  C<create> can be used to
slightly increase performance when creating documents that are known not
to exists in the index.

=back

See also: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/index>
and L</"put_mapping()">


=head3 C<set()>

C<set()> is a synonym for L</"index()">


=head3 C<create()>

C<create> is a synonym for L</"index()"> but creates instead of first checking
whether the doc already exists. This speeds up the indexing process.

=head3 C<get()>

    $result = $e->get(
        index   => single,
        type    => single,
        id      => single,
    );

Returns the document stored at C<index/type/id> or throws an exception if
the document doesn't exist.

Example:

    $e->get( index => 'twitter', type => 'tweet', id => 1)
    Returns:
    {
      _id     => 1,
      _index  => "twitter",
      _source => {
                   message => "trying out Elastic Search",
                   post_date=> "2009-11-15T14:12:12",
                   user => "kimchy",
                 },
      _type   => "tweet",
    }

See also: L<KNOWN ISSUES>,
          L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/get>

=head3 C<delete()>

    $result = $e->delete(
        index   => single,
        type    => single,
        id      => single,
    );

Deletes the document stored at C<index/type/id> or throws an exception if
the document doesn't exist.

Example:

    $e->delete( index => 'twitter', type => 'tweet', id => 1);

See also: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/delete>

=cut

=head2 Query commands

=head3 C<search()>

    $result = $e->search(
        index           => multi,
        type            => multi,
        query           => {query},
        search_type     => $search_type             # optional
        explain         => 1 | 0                    # optional
        facets          => { facets }               # optional
        fields          => [$field_1,$field_n]      # optional
        from            => $start_from              # optional
        size            => $no_of_results           # optional
        sort            => ['score',$field_1]       # optional
        scroll          => '5m' | '30s'             # optional
        highlight       => { highlight }            # optional
        indices_boost   => { index_1 => 1.5,... }   # optional
    );

Searches for all documents matching the query. Documents can be matched
against multiple indices and multiple types, eg:

    $result = $e->search(
        index   => undef,               # all
        type    => ['user','tweet'],
        query   => { term => {user => 'kimchy' }}
    );

For all of the options that can be included in the C<query> parameter, see
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/search> and
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/>

=head3 C<scroll()>

    $result = $e->scroll(scroll_id => $scroll_id );

If a search has been executed with a C<scroll> parameter, then the returned
C<scroll_id> can be used like a cursor to scroll through the rest of the
results.

Note - this doesn't seem to work correctly in version 0.6.0 of ElasticSearch.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/search/#Scrolling>

=head3 C<count()>

    $result = $e->count(
        index           => multi,
        type            => multi,

        bool
      | constantScore
      | disMax
      | field
      | filtered
      | flt
      | flt_field
      | match_all
      | mlt
      | mlt_field
      | query_string
      | prefix
      | range
      | term
      | wildcard
    );

Counts the number of documents matching the query. Documents can be matched
against multiple indices and multiple types, eg

    $result = $e->count(
        index   => undef,               # all
        type    => ['user','tweet'],
        query   => { term => {user => 'kimchy' }}
    );

See also L</"search()">,
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/count>
and L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/>


=head3 C<delete_by_query()>

    $result = $e->delete_by_query(
        index           => multi,
        type            => multi,

        bool
      | constantScore
      | disMax
      | field
      | filtered
      | flt
      | flt_field
      | match_all
      | mlt
      | mlt_field
      | query_string
      | prefix
      | range
      | term
      | wildcard
    );

Deletes any documents matching the query. Documents can be matched against
multiple indices and multiple types, eg

    $result = $e->delete_by_query(
        index   => undef,               # all
        type    => ['user','tweet'],
        term    => {user => 'kimchy' }
    );

See also L</"search()">,
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/delete_by_query>
and L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/>


=head3 C<terms()>

    $terms = $e->terms(
        index           => multi,
        fields          => 'field' | [field1..],   # required
        min_freq        =>  integer,               # optional
        max_freq        =>  integer,               # optional
        size            =>  integer,               # optional
        sort            =>  term (default) | freq  # optional

        ## A range of terms eg alpha - omega
        from            => 'first term',           # optional
        to              => 'last term',            # optional
        from_inclusive  => 1 | 0                   # optional
        to_inclusive    => 1 | 0                   # optional

        prefix          => 'prefix',               # terms starting with
        regexp          => 'regexp',               # terms matching ^regexp$
    );

Get terms (from one or more indices) and the number of times those terms
appear in a document.  Useful for generating tag clouds or for auto-suggestion.

eg:

    $terms = $e->terms(
        index       => 'twitter',
        fields      => ['tweet','reply'],
        prefix      => 'arnol'
    )

See also L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/terms>
and L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/>

=head3 C<mlt()>

    # mlt == more_like_this

    $results = $e->mlt(
        index               => single,              # required
        type                => single,              # required
        id                  => $id,                 # required

        # optional more-like-this params
        boost_terms          =>  float
        mlt_fields           =>  'scalar' or ['scalar_1', 'scalar_n']
        max_doc_freq         =>  integer
        max_query_terms      =>  integer
        max_word_len         =>  integer
        min_doc_freq         =>  integer
        min_term_freq        =>  integer
        min_word_len         =>  integer
        pct_terms_to_match   =>  float
        stop_words           =>  'scalar' or ['scalar_1', 'scalar_n']

        # optional search params
        scroll               =>  '5m' | '10s'
        search_type          =>  "predefined_value"
        explain              =>  {explain}
        facets               =>  {facets}
        fields               =>  {fields}
        from                 =>  {from}
        highlight            =>  {highlight}
        size                 =>  {size}
        sort                 =>  {sort}

    )

More-like-this (mlt) finds related/similar documents. It is possible to run
a search query with a C<moreLikeThis> clause (where you pass in the text
you're trying to match), or to use this method, which uses the text of
the document referred to by C<index/type/id>.

This gets transformed into a search query, so all of the search parameters
are also available.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/more_like_this/>
and L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/moreLikeThis_query/>

=cut

=head2 Index Admin methods

=head3 C<index_status()>

    $result = $e->index_status(
        index   => multi,
    );

Returns the status of
    $result = $e->index_status();                               #all
    $result = $e->index_status( index => ['twitter','buzz'] );
    $result = $e->index_status( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/status>

=head3 C<create_index()>

    $result = $e->create_index(
        index   => single,
        defn    => {...}        # optional
    );

Creates a new index, optionally setting certain paramters, eg:

    $result = $e->create_index(
        index   => 'twitter',
        defn    => {
                number_of_shards      => 3,
                number_of_replicas    => 2,
        }
    );

Throws an exception if the index already exists.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/create_index>

=head3 C<delete_index()>

    $result = $e->delete_index(
        index   => single
    );

Deletes an existing index, or throws an exception if the index doesn't exist, eg:

    $result = $e->delete_index( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/delete_index>

=head3 C<aliases()>

    $result = $e->aliases( actions => [actions] | {actions} )

Adds or removes an alias for an index, eg:

    $result = $e->aliases( actions => [
                { remove => { index => 'foo', alias => 'bar' }},
                { add    => { index => 'foo', alias => 'baz'  }}
              ]);

C<actions> can be a single HASH ref, or an ARRAY ref containing multiple HASH
refs.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/aliases/>

=head3 C<get_aliases()>

    $result = $e->get_aliases( index => multi )

Returns a hashref listing all indices and their corresponding aliases, and
all aliases and their corresponding indices, eg:

    {
      aliases => {
         bar => ["foo"],
         baz => ["foo"],
      },
      indices => { foo => ["baz", "bar"] },
    }

If you pass in the optional C<index> argument, which can be an index name
or an alias name, then it will only return the indices and aliases related
to that argument.

=head3 C<flush_index()>

    $result = $e->flush_index(
        index   => multi
    );

Flushes one or more indices. The flush process of an index basically frees
memory from the index by flushing data to the index storage and clearing the
internal transaction log. By default, ElasticSearch uses memory heuristics
in order to automatically trigger flush operations as required in order to
clear memory.

Example:

    $result = $e->flush_index( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/flush>

=head3 C<refresh_index()>

    $result = $e->refresh_index(
        index   => multi
    );

Explicitly refreshes one or more indices, making all operations performed
since the last refresh available for search. The (near) real-time capabilities
depends on the index engine used. For example, the robin one requires
refresh to be called, but by default a refresh is scheduled periodically.

Example:

    $result = $e->refresh_index( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/refresh>

=head3 C<clear_cache()>

    $result = $e->clear_cache( index => multi );

Clears the caches for the specified indices (currently only the filter cache).

See L<http://github.com/elasticsearch/elasticsearch/issues/issue/101>

=head3 C<gateway_snapshot()>

    $result = $e->gateway_snapshot(
        index   => multi
    );

Explicitly performs a snapshot through the gateway of one or more indices
(backs them up ). By default, each index gateway periodically snapshot changes,
though it can be disabled and be controlled completely through this API.

Example:

    $result = $e->gateway_snapshot( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/gateway_snapshot>
and L<http://www.elasticsearch.com/docs/elasticsearch/modules/gateway>

=head3 C<snapshot_index()>

C<snapshot_index()> is a synonym for L</"gateway_snapshot()">

=head3 C<optimize_index()>

    $result = $e->optimize_index(
        index           => multi,
        only_deletes    => 1 | 0,  # only_expunge_deletes
        flush           => 1 | 0,  # flush after optmization
        refresh         => 1 | 0,  # refresh after optmization
    )

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/optimize>

=head3 C<put_mapping()>

    $result = $e->put_mapping(
        index               => multi,
        type                => single,
        _all                => { ... },
        properties          => { ... },      # required
        timeout             => '5m' | '10s', # optional
        ignore_conflicts    => 1 | 0,        # optional
    );

A C<mapping> is the data definition of a C<type>.  If no mapping has been
specified, then ElasticSearch tries to infer the types of each field in
document, by looking at its contents, eg

    'foo'       => string
    123         => integer
    1.23        => float

However, these heuristics can be confused, so it safer (and much more powerful)
to specify an official C<mapping> instead, eg:

    $result = $e->put_mapping(
        index   => ['twitter','buzz'],
        type    => 'tweet',
        properties  =>  {
            user        =>  {type  =>  "string", index      =>  "not_analyzed"},
            message     =>  {type  =>  "string", nullValue  =>  "na"},
            post_date   =>  {type  =>  "date"},
            priority    =>  {type  =>  "integer"},
            rank        =>  {type  =>  "float"}
        }
    );

See also: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/put_mapping>
and L<http://www.elasticsearch.com/docs/elasticsearch/mapping>

=head3 C<get_mapping()>

    $mapping = $e->get_mapping(
        index       => single,
        type        => multi
    );

Returns the mappings for all types in an index, or the mapping for the specified
type(s), eg:

    $mapping = $e->get_mapping(
        index       => 'twitter',
        type        => 'tweet'
    );

    $mappings = $e->get_mapping(
        index       => 'twitter',
        type        => ['tweet','user']
    );
    # { tweet => {mapping}, user => {mapping}}

The index argument must be an index name, and not an alias name.

=cut

=head2 Cluster admin methods

=head3 C<cluster_state()>

    $result = $e->cluster_state();

Returns cluster state information.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/state/>

=head3 C<cluster_health()>

    $result = $e->cluster_health(
        index                         => multi,
        level                         => 'cluster' | 'indices' | 'shards',
        wait_for_status               => 'red' | 'yellow' | 'green',
        | wait_for_relocating_shards  => $number_of_shards,
        timeout                       => $seconds
    );

Returns the status of the cluster, or index|indices or shards, where the
returned status means:

=over

=item red: Data not allocated

=item yellow: Primary shard allocated

=item green: All shards allocated

=back

It can block to wait for a particular status (or better), or can block to
wait until the specified number of shards have been relocated (where 0 means
all).

If waiting, then a timeout can be specified.

For example:

    $result = $e->cluster_health( wait_for_status => 'green', timeout => 10)

=head3 C<nodes()>

    $result = $e->nodes(
        nodes       => multi,
        settings    => 1 | 0        # optional
    );

Returns information about one or more nodes or servers in the cluster. If
C<settings> is C<true>, then it includes the node settings information.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_info>

=head3 C<nodes_stats()>

    $result = $e->nodes_stats(
        nodes       => multi,
    );

Returns various statistics about one or more nodes in the cluster.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_stats/>

=head3 C<shutdown()>

    $result = $e->shutdown(
        nodes       => multi,
        delay       => '5s' | '10m'        # optional
    );


Shuts down one or more nodes (or the whole cluster if no nodes specified),
optionally with a delay.

C<node> can also have the values C<_local>, C<_master> or C<_all>.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_shutdown/>

=head3 C<restart()>

    $result = $e->restart(
        nodes       => multi,
        delay       => '5s' | '10m'        # optional
    );


Restarts one or more nodes (or the whole cluster if no nodes specified),
optionally with a delay.

C<node> can also have the values C<_local>, C<_master> or C<_all>.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_restart>

=cut

=head2 Module-specific methods

=head3 C<servers()>

    $servers = $e->servers

Returns a list of the servers/nodes known to be in the cluster the last time
that L</"refresh_servers()"> was called.

=head3 C<refresh_servers()>

    $e->refresh_servers( $server | [$server_1, ...$server_n])
    $e->refresh_servers()

Tries to contact each server in the list to retrieve a list of servers/nodes
currently in the cluster. If it succeeds, then it updates L</"servers()"> and
randomly selects one server to be the L</"current_server()">

If no servers are passed in, then it uses the list from L</"servers()"> (ie
the last known good list) instead.

Throws an exception if no servers can be found.

C<refresh_server> is called from :

=over

=item L</"new()">

=item if any L</"request()"> fails

=item if the process forks and the PID changes

=back

=head3 C<current_server()>

    $current_server = $e->current_server()

Returns the current server for the current PID, or if none is set, then it
tries to get a new current server by calling L</"refresh_servers()">.

=head3 C<ua()>

    $ua = $e->ua

Returns the current L<LWP::UserAgent> instance for the current PID.  If there is
none, then it creates a new instance, with any options specified in
L</"ua_options()">

C<Keep-alive> is used by default (via L<LWP::ConnCache>).

=head3 C<ua_options()>

    $ua_options = $e->ua({....})

Get/sets the current list of options to be used when creating a new
C<LWP::UserAgent> instance.  You may, for instance, want to set C<timeout>

This is best set when creating a new instance of ElasticSearch with L</"new()">.

=cut

=head3 C<JSON()>

    $json_xs = $e->JSON

Returns the current L<JSON::XS> object which is used to encode and decode
all JSON when communicating with ElasticSearch.

If you need to change the JSON settings you can do (eg):

    $e->JSON->utf8

It is probably better not to fiddle with this!  ElasticSearch expects all
data to be provided as Perl strings (not as UTF8 encoded byte strings) and
returns all data from ElasticSearch as Perl strings.

=head3 C<camel_case()>

    $bool = $e->camel_case($bool)

Gets/sets the camel_case flag. If true, then all JSON keys returned by
ElasticSearch are in camelCase, instead of with_underscores.  This flag
does not apply to the source document being indexed or fetched.

Defaults to false.

=cut

=head3 C<request()>

    $result = $e->request({
        method  => 'GET|PUT|POST|DELETE',
        cmd     => url,                     # eg '/twitter/tweet/123'
        data    => $hash_ref                # converted to JSON document
    })

The C<request()> method is used to communicate with the ElasticSearch
L</"current_server()">.  If any request fails with a C<Can't connect> error,
then C<request()> tries to refresh the server list, and repeats the request.

Any other error will throw an exception.

=head3 C<throw()>

    $e->throw('ErrorType','ErrorMsg', {vars})

Throws an exception of C<ref $e . '::Error::' . $error_type>, eg:

    $e->throw('Param', 'Missing required param', { params => $params})

... will thrown an error of class C<ElasticSearch::Error::Param>.

Any vars passed in will be available as C<< $error->{-vars} >>.

If L</"debug()"> is C<true>, then C<< $error->{-stacktrace} >> will contain a
stacktrace.

=head3 C<debug()>

    $e->debug(1|0);

If C<debug()> is C<true>, then exceptions include a stack trace.

=head3 C<trace_calls()>

    $es->trace_calls(1);            # log to STDERR
    $es->trace_calls($filename);    # log to $filename
    $es->trace_calls(0 | undef);    # disable logging

C<trace_calls()> is used for debugging.  All requests to the cluster
are logged either to C<STDERR> or the specified file, in a form that can
be rerun with curl.

The cluster response will also be logged, and commented out.

Example: C<< $e->nodes() >> is logged as:

    curl -XGET http://127.0.0.1:9200/_cluster/nodes
    # {
    #    "clusterName" : "elasticsearch",
    #    "nodes" : {
    #       "getafix-24719" : {
    #          "http_address" : "inet[/127.0.0.2:9200]",
    #          "data_node" : true,
    #          "transport_address" : "inet[getafix.traveljury.com/127.0.
    # >         0.2:9300]",
    #          "name" : "Sangre"
    #       },
    #       "getafix-17782" : {
    #          "http_address" : "inet[/127.0.0.2:9201]",
    #          "data_node" : true,
    #          "transport_address" : "inet[getafix.traveljury.com/127.0.
    # >         0.2:9301]",
    #          "name" : "Williams, Eric"
    #       }
    #    }
    # }


=cut

=head1 AUTHOR

Clinton Gormley, C<< <drtech at cpan.org> >>

=head1 KNOWN ISSUES

=over

=item   L</"set()">, L</"index()"> and L</"create()">

If one of the fields that you are trying to index has the same name as the
type, then you need change the format as follows:

Instead of:

     $e->set(index=>'twitter', type=>'tweet',
             data=> { tweet => 'My tweet', date => '2010-01-01' }
     );

you should include the type name in the data:

     $e->set(index=>'twitter', type=>'tweet',
             data=> { tweet=> { tweet => 'My tweet', date => '2010-01-01' }}
     );

=item   L</"get()">

The C<_source> key that is returned from a L</"get()"> contains the original JSON
string that was used to index the document initially.  ElasticSearch parses
JSON more leniently than L<JSON::XS>, so if invalid JSON is used to index the
document (eg unquoted keys) then C<< $e->get(....) >> will fail with a
JSON exception.

Any documents indexed via this module will be not susceptible to this problem.

=item L</"scroll()">

C<scroll()> is broken in version 0.6.0 of ElasticSearch.

See L<http://github.com/elasticsearch/elasticsearch/issues/issue/136>

=back

=head1 BUGS

This is an alpha module, so there will be bugs, and the API is likely to
change in the future, as the API of ElasticSearch itself changes.

If you have any suggestions for improvements, or find any bugs, please report
them to L<http://github.com/clintongormley/ElasticSearch.pm/issues>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 TODO

Hopefully I'll be adding an ElasticSearch::QueryBuilder (similar to
L<SQL::Abstract>) which will make it easier to generate valid queries
for ElasticSearch.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc ElasticSearch


You can also look for information at:

=over 4

=item * GitHub

L<http://github.com/clintongormley/ElasticSearch.pm>

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=ElasticSearch>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/ElasticSearch>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/ElasticSearch>

=item * Search CPAN

L<http://search.cpan.org/dist/ElasticSearch/>

=back


=head1 ACKNOWLEDGEMENTS

Thanks to Shay Bannon, the ElasticSearch author, for producing an amazingly
easy to use search engine.

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
