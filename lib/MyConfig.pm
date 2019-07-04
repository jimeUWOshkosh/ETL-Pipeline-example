package MyConfig;
use strict;
use warnings;

# all the Mojolicious models for this project are modulinos.
# They need a library module to read in the project's
# configuration file to connect to the RDBMS

BEGIN {
    require Exporter;
    our $VERSION = 1.00;
    our @ISA     = qw(Exporter);     ## no critic
    our @EXPORT  = qw(GetDSNinfo);
}

use Try::Tiny;
use Carp 'croak';
use Config::Any::Perl;

sub GetDSNinfo {                     ## no critic
    my ($file) = @_;
    my ( $rc1, $rc2, $rc3, $rc4 );
    try {
        my $cfg = Config::Any::Perl->load($file);
        $rc1 = $cfg->{DSN};
        $rc2 = $cfg->{USER};
        $rc3 = $cfg->{PASSWORD};
        $rc4 = $cfg->{DBEXTRA};
    }
    catch {
        croak $_ ;                   # rethrow
    };
    return $rc1, $rc2, $rc3, $rc4;
}
1;

=head1 NAME

MyConfig - Grab the variables to connect to the RDBMS

=head1 VERSION

This documentation refers to MyConfig version 1.00

=head1 SYNOPSIS

   use MyConfig 'GetDSNinfo';

   my ( $dsn, $u, $p, $extra ) = GetDSNinfo('up.conf');

=head1 DESCRIPTION

All the Mojolicious models for this project are modulinos.
They need a library module to read in the project's
configuration file to connect to the RDBMS when executed
from the command line.

The test programs that need to connect to the database
use this module also.

=head1 SUBROUTINES/METHODS

=head2 GetDSNinfo

Read in the Mojolicious configuration file and pass back
the DBvendor:DatabaseName, User, Password, extra connection info.

=head1 DIAGNOSTICS

=head2 Possible errors

  Failed to find configuration file

=head1 CONFIGURATION AND ENVIRONMENT

The configuration file for this Mojolicious application is 'up.conf' in
the project's home directory.

=head1 DEPENDENCIES

View CPAN.DEPENDENCIES in the project's home directory

=head1 INCOMPATIBILITIES

None to be reported at this time

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.

Please report problems.

Patches are welcome.

Contact via github account you found this code at.

=head1 AUTHOR

Jim Edwards

=head1 LICENSE AND COPYRIGHT

Since this a proof of concept there is no license or copyright.

Help other Perl programmers out by posting full examples
of your hard testing/work to a github like repository.

