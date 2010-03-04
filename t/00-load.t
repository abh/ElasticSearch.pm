#!perl

use Test::Most qw(defer_plan);
use Module::Build;
use File::Spec::Functions qw(catfile);
use POSIX 'setsid';

BEGIN {
    use_ok 'ElasticSearch' || print "Bail out!";
}

diag("Testing ElasticSearch $ElasticSearch::VERSION, Perl $], $^X");

local $SIG{INT} = sub { shutdown_servers(); };

my $es      = connect_to_es();
my $Index   = 'es_test_1';
my $Index_2 = 'es_test_2';

SKIP: {

    skip "No ElasticSearch server available", 1
        unless $es;
    ok $es, 'Connected to an ES cluster';
    like $es->current_server, qr{http://}, 'Current server set';

    ### CLUSTER STATE ###

    my $r;

    isa_ok $r = $es->cluster_state, 'HASH', 'Cluster state';
    isa_ok $r->{metadata},     'HASH', ' - has metadata';
    isa_ok $r->{routingNodes}, 'HASH', ' - has routingNodes';
    isa_ok $r->{routingTable}, 'HASH', ' - has routingTable';

    ### NODES ###
    isa_ok $r = $es->nodes, 'HASH', 'All nodes';

    ok $r->{clusterName}, ' - has clusterName';
    isa_ok $r->{nodes}, 'HASH', ' - has nodes';

    my @nodes     = ( keys %{ $r->{nodes} } );
    my $num_nodes = @nodes;

    isa_ok $r = $es->nodes( node => $nodes[0] ), 'HASH', ' - single node';
    is keys %{ $r->{nodes} }, 1, ' - retrieved one node';
    ok $r->{nodes}{ $nodes[0] }, ' - retrieved the same node';

    isa_ok $es->nodes( settings => 1 )->{nodes}{ $nodes[0] }{settings},
        'HASH', ' - node settings';

SKIP: {
        skip "Requires more than 2 nodes ", 3 unless $num_nodes > 2;

        # remove one, so we're not just retrieving all nodes
        shift @nodes;
        isa_ok $r = $es->nodes( node => \@nodes ), 'HASH', ' - nodes by name';
        is keys %{ $r->{nodes} }, @nodes, ' - retrieved same number of nodes';
        is_deeply [ keys %{ $r->{nodes} } ],
            \@nodes, ' - retrieved the same nodes';
    }

    # drop index in case rerunning test
    drop_indices();

    ### CREATE INDEX ###
    ok $es->create_index( index => $Index )->{ok}, 'Created index';
    throws_ok { $es->create_index( index => $Index ) } qr/Already exists/,
        ' - second create fails';

    throws_ok { $es->create_index( index => [ $Index, $Index_2 ] ) }
    qr/must be a single value/,
        ' - multiple indices fails';

    ok $r = $es->create_index( index => $Index_2,
                               defn  => {numberOfShards   => 3,
                                         numberOfReplicas => 1
                               }
        )->{ok},
        'Create index with defn';

    wait_for_es();

    ### INDEX STATUS ###
    my $indices;
    ok $indices = $es->index_status()->{indices}, 'Index status - all';
    ok $indices->{$Index},   ' - Index 1 exists';
    ok $indices->{$Index_2}, ' - Index 2 exists';

    is_deeply $es->cluster_state->{metadata}{indices}{$Index_2}{settings},
        { "index.numberOfReplicas" => 1, "index.numberOfShards" => 3 },
        ' - Index 2 defn';

    ### DELETE INDEX ###
    ok $es->delete_index( index => $Index_2 )->{ok}, 'Delete index';

    throws_ok { $es->index_status( index => $Index_2 ) } qr/\[.*\] missing/,
        ' - index deleted';

    $es->create_index( index => $Index_2 );

    ### REFRESH INDEX ###
    ok $es->refresh_index()->{ok}, 'Refresh index';

    ### FLUSH INDEX ###
    ok $es->flush_index()->{ok}, 'Flush index';
    ok $es->flush_index( refresh => 1 )->{ok}, ' - with refresh';

    ### OPTIMIZE INDEX ###
    ok $es->optimize_index()->{ok}, 'Optimize all indices';
    ok $es->optimize_index( only_deletes => 1 )->{ok}, ' - only_deletes';
    ok $es->optimize_index( flush        => 1 )->{ok}, ' - with flush';
    ok $es->optimize_index( refresh      => 1 )->{ok}, ' - with refresh';
    ok $es->optimize_index( flush => 1, refresh => 1, only_deletes => 1 )
        ->{ok},
        ' - with flush, refresh and only_deletes';

    ok $es->optimize_index( index => [ $Index, $Index_2 ] )->{ok},
        'Optimize test indices';
    ok $es->optimize_index( index        => [ $Index, $Index_2 ],
                            only_deletes => 1
        )->{ok},
        ' - only_deletes';
    ok $es->optimize_index( index => [ $Index, $Index_2 ], flush => 1 )
        ->{ok},
        ' - with flush';
    ok $es->optimize_index( index => [ $Index, $Index_2 ], refresh => 1 )
        ->{ok},
        ' - with refresh';
    ok $es->optimize_index( index        => [ $Index, $Index_2 ],
                            flush        => 1,
                            refresh      => 1,
                            only_deletes => 1
        )->{ok},
        ' - with flush, refresh and only_deletes';

    ### SNAPSHOT INDEX ###
    ok $es->snapshot_index()->{ok},   'Snapshot all indices';
    ok $es->gateway_snapshot()->{ok}, ' - with gateway_snapshot';
    ok $es->snapshot_index( index => [ $Index, $Index_2 ], )->{ok},
        'Snapshot test indices';
    ok $es->gateway_snapshot( index => [ $Index, $Index_2 ] )->{ok},
        ' - with gateway_snapshot';

    ### INDEX DOCUMENTS ###
    isa_ok $r= $es->index( index => $Index,
                           type  => 'test',
                           id    => 1,
                           data  => {text => '123',
                                     num  => 'foo'
                           }
        ),
        'HASH', 'Index document';
    ok $r->{ok}, ' - Indexed';
    is $r->{_id}, 1, ' - ID matches';
    wait_for_es(1);

    isa_ok $r= $es->get( index => $Index, type => 'test', id => 1 ), 'HASH',
        'Get document';
    is $r->{_id}, 1, ' - ID matches';
    is $r->{_source}{num}, 'foo', ' - data matches';

    is $es->search( index => $Index,
                    type  => 'test',
                    query => { term => { num => 'foo' } }
        )->{hits}{total},
        1,
        ' - retrieved with query';

    ### CREATE DOCUMENTS ###
    isa_ok $r= $es->create( index => $Index,
                            type  => 'test',
                            id    => 1,
                            data  => {text => '123',
                                      num  => 'foo'
                            }
        ),
        'HASH', 'Create document';

    wait_for_es(1);
    ok $r->{ok}, ' - Created';
    is $r->{_id}, 1, ' - ID matches';
    is $es->search( index => $Index,
                    type  => 'test',
                    query => { term => { num => 'foo' } }
        )->{hits}{total},
        2,
        ' - retrieved both copies query';

    isa_ok $r= $es->set( index => $Index,
                         type  => 'test',
                         id    => 1,
                         data  => {text => '123',
                                   num  => 'foo'
                         }
        ),
        'HASH', ' - re-set document';

    wait_for_es(1);
    is $es->search( index => $Index,
                    type  => 'test',
                    query => { term => { num => 'foo' } }
        )->{hits}{total},
        1,
        ' - now only single copy';

    ### PUT MAPPING ###
    drop_indices();

    ok $es->put_mapping(
           index => [ $Index, $Index_2 ],
           type  => 'test',
           properties =>
               { text => { type => 'string' }, num => { type => 'integer' } },
        ),
        'Create mapping';

    isa_ok my $mapping = $es->get_mapping( index => $Index, type => 'test' ),
        'HASH', ' - index 1 mapping exists';

    is $mapping->{properties}{text}{type}, 'string',  ' - index 1 string map';
    is $mapping->{properties}{num}{type},  'integer', ' - index 1 int map';

    throws_ok {
        $es->put_mapping( index      => [ $Index, $Index_2 ],
                          type       => 'test',
                          properties => { text => { type => 'string' },
                                          num  => { type => 'integer' }
                          },
                          ignore_conflicts => 0,
        );
    }
    qr/exists, can't merge/, 'Error on duplicate mapping';

    ok $r= $es->put_mapping( index      => [ $Index, $Index_2 ],
                             type       => 'test',
                             properties => { text => { type => 'string' },
                                             num  => { type => 'integer' }
                             }
        ),
        'Ignore duplicate mapping';

    ok $es->put_mapping(
           index => [ $Index, $Index_2 ],
           type  => 'test_2',
           properties =>
               { text => { type => 'string' }, num => { type => 'integer' } },
        ),
        'Create second mapping';

    is join( '-', sort keys %{ $es->get_mapping( index => $Index ) } ),
        'test-test_2', ' - get all mappings';

    is join( '-',
             sort keys %{
                 $es->get_mapping( index => $Index,
                                   type  => [ 'test', 'test_2', 'test_3' ]
                 )
                 }
        ),
        'test-test_2', ' - get list of mappings';

    ok !defined $es->get_mapping( index => $Index, type => 'test_3' ),
        ' - get unknown mapping';

    ### QUERY TESTS ###
    index_test_docs();

    # MATCH ALL
    isa_ok $r= $es->search( query => { matchAll => {} } ), 'HASH',
        "Match all";
    is $r->{hits}{total}, 28, ' - total correct';
    is @{ $r->{hits}{hits} }, 10, ' - returned 10 results';

    # RETRIEVE ALL RESULTS
    isa_ok $r= $es->search( query => { matchAll => {} }, size => 100 ),
        'HASH', "Match up to 100";
    is $r->{hits}{total}, 28, ' - total correct';
    is @{ $r->{hits}{hits} }, 28, ' - returned 28 results';

    # QUERY_THEN_FETCH
    isa_ok $r= $es->search( query       => { matchAll => {} },
                            search_type => 'query_then_fetch'
        ),
        'HASH', "query_then_fetch";
    is $r->{hits}{total}, 28, ' - total correct';
    is @{ $r->{hits}{hits} }, 10, ' - returned 10 results';

SKIP: {

        # QUERY_AND_FETCH
        skip "Requires more than 1 node ", 3 unless $num_nodes > 1;

        isa_ok $r= $es->search( query       => { matchAll => {} },
                                search_type => 'query_and_fetch'
            ),
            'HASH', "query_and_fetch";
        is $r->{hits}{total}, 28, ' - total correct';
        ok @{ $r->{hits}{hits} } > 10, ' - returned  > 10 results';
    }

    # TERM SEARCH
    isa_ok $r= $es->search( query => { term => { text => 'foo' } } ), 'HASH',
        "Match text: foo";
    is $r->{hits}{total}, 16, ' - total correct';

    # QUERY STRING SEARCH
    isa_ok $r =
        $es->search(
          query => {
              queryString => { defaultField => 'text', query => 'foo OR bar' }
          }
        ),
        'HASH', "Match text: bar foo";

    # FACETS SEARCH

    isa_ok $r =
        $es->search(
                   facets => {
                       bazFacet => { query => { term => { text => 'baz' } } },
                       barFacet => { query => { term => { text => 'bar' } } }
                   },
                   query => { term => { text => 'foo' } }
        ),
        'HASH', "Facets search";

    is $r->{hits}{total},      16, ' - total correct';
    is $r->{facets}{bazFacet}, 8,  ' - first facet correct';
    is $r->{facets}{barFacet}, 8,  ' - second facet correct';

    # EXPLAIN SEARCH
    isa_ok $es->search( query => { term => { text => 'foo' } }, explain => 1 )
        ->{hits}{hits}[0]{_explanation}, 'HASH',
        "Query with explain";

    # SORT
    is $es->search( query => { matchAll => {} },
                    sort  => ['num'],
    )->{hits}{hits}[0]{_source}{num}, 2, "Query with sort";

    is $es->search( query => { matchAll => {} },
                    sort => [ { num => { reverse => \1 } } ],
    )->{hits}{hits}[0]{_source}{num}, 29, " - reverse sort";

    # FROM / TO
    ok $r= $es->search( query => { matchAll => {} },
                        sort  => ['num'],
                        size  => 5,
                        from  => 5,
        ),
        "Query with size and from";
    is @{ $r->{hits}{hits} }, 5, ' - number of hits correct';
    is $r->{hits}{hits}[0]{_source}{num}, 7, ' - started from correct pos';

    # FIELDS
    like $es->search( query => { term => { text => 'foo' } },
                      fields => [ 'text', 'num' ]
    )->{hits}{hits}[0]{fields}{text}, qr/foo/, 'Fields query';

    ### COUNT ###

    is $es->count( term => { text => 'foo' } )->{count}, 16, "Count: term";
    is $es->count( range => { num => { from => 10, to => 20 } } )->{count},
        11, 'Count: range';
    is $es->count( prefix => { text => 'ba' } )->{count}, 24, 'Count: prefix';
    is $es->count( wildcard => { text => 'ba?' } )->{count}, 24,
        'Count: wildcard';
    is $es->count( match_all => {} )->{count}, 28, 'Count: matchAll';
    is $es->count( query_string =>
                 { query => 'foo AND bar AND -baz', defaultField => 'text' } )
        ->{count}, 4, 'Count: queryString';
    is $es->count( bool => { must => [ { term => { text => 'foo' } },
                                       { term => { text => 'bar' } }
                             ]
                   }
    )->{count}, 8, 'Count: bool';
    is $es->count( dis_max => { queries => [ { term => { text => 'foo' } },
                                             { term => { text => 'bar' } }
                                ]
                   }
    )->{count}, 24, 'Count: disMax';
    is $es->count(
               constant_score => { filter => { term => { text => 'foo' } } } )
        ->{count}, 16, 'Count: constantScore';
    is $es->count( filtered_query => { query => { term => { text => 'foo' } },
                                       filter => { term => { text => 'bar' } }
                   }
    )->{count}, 8, 'Count: filteredQuery';

    ### TERMS
    # add another foo to make the document frequency uneven
    $es->set( index => $Index,
              type  => 'type_1',
              id    => 30,
              data  => { text => 'foo' }
    );
    wait_for_es(1);

    isa_ok $r= $es->terms( fields => 'text' )->{fields}{text}{terms},
        'ARRAY', "All terms";
    is $r->[2]{docFreq}, 17, ' - foo docFreq';
    is $r->[0]{docFreq}, 16, ' - bar docFreq';

    is $es->terms( index => $Index, fields => 'text' )
        ->{fields}{text}{terms}[2]{docFreq}, 9, ' - foo on index 1';

    is $es->terms( fields => 'text', min_freq => 17 )
        ->{fields}{text}{terms}[0]{term}, 'foo', ' - minFreq';
    is $es->terms( fields => 'text', max_freq => 16 )
        ->{fields}{text}{terms}[-1]{term}, 'baz', ' - maxFreq';

    is @{ $es->terms( fields => 'text', size => 2 )->{fields}{text}{terms} },
        2, ' - size';
    is $es->terms( fields => 'text', sort => 'freq' )
        ->{fields}{text}{terms}[0]{term}, 'foo', ' - sort freq';

    is join( '-',
             map { $_->{term} }
                 @{
                 $es->terms( fields => 'text', from => 'baz' )
                     ->{fields}{text}{terms}
                 }
        ),
        'baz-foo', ' - from';
    is join( '-',
             map { $_->{term} }
                 @{
                 $es->terms( fields => 'text', to => 'baz' )
                     ->{fields}{text}{terms}
                 }
        ),
        'bar-baz', ' - to';

    is join( '-',
             map { $_->{term} }
                 @{
                 $es->terms( fields       => 'text',
                             from         => 'baz',
                             exclude_from => 1
                     )->{fields}{text}{terms}
                 }
        ),
        'foo', ' - exclude_from';
    is join( '-',
             map { $_->{term} }
                 @{
                 $es->terms( fields     => 'text',
                             to         => 'baz',
                             exclude_to => 1
                     )->{fields}{text}{terms}
                 }
        ),
        'bar', ' - exclude_to';

    is join( '-',
             map { $_->{term} }
                 @{
                 $es->terms( fields => 'text', prefix => 'ba' )
                     ->{fields}{text}{terms}
                 }
        ),
        'bar-baz', ' - prefix';
    is join( '-',
             map { $_->{term} }
                 @{
                 $es->terms( fields => 'text', regexp => 'foo|baz' )
                     ->{fields}{text}{terms}
                 }
        ),
        'baz-foo', ' - regexp';

    ###  DELETE_BY_QUERY ###
    ok $es->delete_by_query( term => { text => 'foo' } )->{ok},
        "Delete by query";
    wait_for_es(1);
    is $es->count( term => { text => 'foo' } )->{count}, 0, " - foo deleted";
    is $es->count( term => { text => 'bar' } )->{count}, 8,
        " - bar not deleted";

}

all_done;

sub index_test_docs {

    ### ADD DOCUMENTS TO TEST QUERIES ###
    diag("Preparing indices for query tests");

    drop_indices();

    $es->create_index( index => $Index );
    $es->create_index( index => $Index_2 );
    $es->put_mapping( type       => 'type_1',
                      properties => {
                                text => { type => 'string',  store => 'yes' },
                                num  => { type => 'integer', store => 'yes' }
                      },
    );

    $es->put_mapping( type       => 'type_2',
                      properties => {
                                text => { type => 'string',  store => 'yes' },
                                num  => { type => 'integer', store => 'yes' }
                      },
    );

    wait_for_es();

    my @phrases = ( 'foo',
                    'foo bar',
                    'foo bar baz',
                    'bar baz',
                    'baz',
                    'bar',
                    'foo baz'
    );

    my $id = 1;
    for my $phrase (@phrases) {
        for my $index ( $Index, $Index_2 ) {
            for my $type (qw(type_1 type_2)) {
                diag("... document $id");
                $es->set( index => $index,
                          type  => $type,
                          id    => $id++,
                          data  => { text => $phrase, num => $id }
                );

            }
        }
    }
    wait_for_es(1);

}

sub drop_indices {
    eval {
        $es->delete_index( index => $Index );
        $es->delete_index( index => $Index_2 );
        wait_for_es();
    };

}

sub wait_for_es {
    $es->cluster_health( wait_for_status => 'green', timeout => 2 );
    sleep $_[0] if $_[0];
}

my @PIDs;

sub connect_to_es {
    my $server = $ENV{ES_SERVER};
    if ( !$server ) {
        my $install_dir = eval {
            require Alien::ElasticSearch;
            return Alien::ElasticSearch->install_dir;
        };
        return unless $install_dir;
        my $cmd = catfile( $install_dir, 'bin', 'elasticsearch' );
        my $pid_file = File::Temp->new;

        for ( 1 .. 3 ) {
            diag "Starting test node $_";
            if (fork) {
                die "Can't start a new session: $!" if setsid == -1;
                exec( $cmd, '-p', $pid_file->filename );

            }
            else {
                sleep 1;
                open my $pid_fh, '<', $pid_file->filename;
                push @PIDs, <$pid_fh>;
            }

        }
        $server = '127.0.0.1:9200';
        diag "Waiting for servers to warm up";
        sleep 8;
    }
    my $es = eval {
        ElasticSearch->new( servers => $server, trace_calls => 'log' );
        }
        or diag("**** Couldn't connect to ElasticServer at $server ****");
    return $es;
}

sub shutdown_servers { kill 9, @PIDs; wait; exit(0) }
END { shutdown_servers() }

