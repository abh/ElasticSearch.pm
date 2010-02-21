use strict;
use warnings;
use Module::Build;

my $installed = eval {
    require Alien::ElasticSearch;
    Alien::ElasticSearch->install_dir;
};

my $install_es;
if ( !$installed && !$ENV{ES_SERVER} ) {
    print <<INSTALL;

    In order to test ElasticSearch, we need a live ElasticSearch
    cluster to test against.

    If you already have ElasticSearch installed, then please start
    3 nodes, and set the environment variable ES_VARIABLE to the
    URL for the cluster, before rerunning $0, eg:

        ES_SERVER="127.0.0.1:9200" ./Build test

    I can install a test server for you (choose 'i' for 'install'),
    or you can skip testing (choose 's' for 'skip').

INSTALL

    my $answer
        = Module::Build->prompt( "[s]kip, [q]uit, or [i]nstall:", 'i' );
    exit(0) if $answer =~ /q/i;
    $install_es = $answer =~ /i/i;
}

my $builder = Module::Build->new(
                        module_name => 'ElasticSearch',
                        license     => 'perl',
                        dist_author => q{Clinton Gormley <drtech@cpan.org>},
                        dist_version_from => 'lib/ElasticSearch.pm',
                        build_requires    => {
                            'Test::Most' => 0,
                            $install_es ? ( 'Alien::ElasticSearch' => 0 ) : ()
                        },
                        add_to_cleanup     => ['ElasticSearch-*'],
                        create_makefile_pl => 'traditional',
                        requires           => {
                                      'LWP::UserAgent' => 0,
                                      'LWP::ConnCache' => 0,
                                      'HTTP::Request'  => 0,
                                      'JSON::XS'       => 0,
                                      'Data::Dump'     => 0,
                                      'Encode'         => 0,
                        }
);

$builder->create_build_script();
