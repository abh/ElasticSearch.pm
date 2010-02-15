#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'ElasticSearch' ) || print "Bail out!
";
}

diag( "Testing ElasticSearch $ElasticSearch::VERSION, Perl $], $^X" );
