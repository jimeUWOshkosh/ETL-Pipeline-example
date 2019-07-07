package DBrec;

# use strict; Moose brings in strict
# use warnings; Moose brings in warnings

use Moose;

use feature 'say';
use namespace::autoclean;

our $VERSION = '1.00';

=head1 NAME

DBrec - holds a dbh and a mesg

=head1 VERSION

version 1.00

=head1 AUTHOR

Jim Edwards

=head1 SYNOPSIS

 use DBrec;

 my $rec = DBrec->new( dt => $dbh, $mesg => '');

=head1 DESCRIPTION

An Object to hold info about an individual tweet.

=head1 Accessor(s)

=head2 mesg

  data_type:    Str

=head1 Default Attribute(s)

=head2 dt

  data_type:    Object

=cut

has 'mesg', is => 'rw', isa => 'Str';
has 'dt',   is => 'rw', isa => 'Object';

=head1 SUBROUTINES/METHODS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 LICENSE AND COPYRIGHT

Ya Right

=cut

__PACKAGE__->meta->make_immutable;

1;
