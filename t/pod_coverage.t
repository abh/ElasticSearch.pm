#!perl

use strict;
use warnings;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}

# Ensure a recent version of Test::Pod
my $min_tp = 1.08;
eval "use Test::Pod::Coverage $min_tp";
plan skip_all => "Test::Pod::Coverage $min_tp required for testing POD Coverage" if $@;

all_pod_coverage_ok();
