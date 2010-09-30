package ElasticSearch::Transport::Base;

use strict;
use warnings FATAL => 'all';
use Scalar::Util;

#===================================
sub new {
#===================================
    my $class = shift;
    my $es = shift || die "No ElasticSearch object passed to ${class}::new()";
    my $self = {_es => $es};
    Scalar::Util::weaken($self->{es});
    return bless $self,$class;
}

#===================================
sub es { shift->{_es}}
#===================================

1;
