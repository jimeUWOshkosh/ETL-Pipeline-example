package Local::EExlsx001;
use strict;
use warnings;
use feature 'say';
use utf8::all;
use lib 'lib';
use Moose;
use namespace::autoclean;
with 'ETL::Pipeline::Output::Storage::Hash';
with 'ETL::Pipeline::Output';
use feature qw(signatures);
no warnings qw(experimental::signatures);    ## no critic
use Up::Schema;
use MooseX::Types::LoadableClass;
use Encode qw(decode encode);
use Ouch;
use Data::Dumper;
use Try::Tiny;
use DBIx::Class::Storage::TxnScopeGuard;
use DBrec;

our $VERSION = '1.00';

our ( $dbh, $fn, $file_id, $sheet_id );

has 'dbinfo' => (
    is       => 'ro',
    isa      => 'DBrec',
    required => 1,
);

sub configure ($self) {

    my $tmp = $self->dbinfo;
    $dbh = $tmp->dt;
    $fn = $self->pipeline->input->file->{file};
    return;
}

sub finish {
    return;
}

sub default_fields {
    return;
}

sub write_record ($self) {

    my $ra_rec  = $self->pipeline->output->current;
    my $rec_num = $self->pipeline->input->record_number;

    try {
        if ( $rec_num == 2 ) {
            my $n_dataset_rs = $dbh->resultset('Dataset')->create( {file => $fn,} );
            $file_id = $n_dataset_rs->file_id;
            my $datasheet_rs = $dbh->resultset('Datasheet')->create(
                {
                    file_id    => $file_id,
                    sheet_indx => 1,
                    sheet_name => 'Sheet 1',
                }
            );
            $sheet_id = $datasheet_rs->sheet_id;
        }

        $ra_rec->{name} = decode( 'UTF-8', $ra_rec->{name}, Encode::FB_CROAK );
        $ra_rec->{utf}  = decode( 'UTF-8', $ra_rec->{utf},  Encode::FB_CROAK );

        my $data_rs = $dbh->resultset('Data')->create(
            {
                file_id  => $file_id,
                sheet_id => $sheet_id,
                row_indx => $rec_num,
                name     => $ra_rec->{name},
                age      => $ra_rec->{age},
                utf      => $ra_rec->{utf},
            }
        );
    } catch {
	my ($str) = $_ =~ m/constraint failed: (.*) \[/;
	if ($str) {
	    $self->dbh->mesg = "Missing record field(s): $str";
	    ouch "SQL Error", "Missing record field(s): $str";
        } else {
	    $self->dbh->mesg = "Other DB error: $_";
	    ouch "SQL Error", "Other DB error: $_";
	}
    };
    return 1;
}
sub new_record { return; }

__PACKAGE__->meta->make_immutable;
1;

=head1 NAME

Local::EExlsx001 - Handles the T&L portion of ETL for XLSX files.
For this project, it will insert a header (dataset), sub header (datasheet),
and line items (data) into the database. These are pseudo employee records
for bogus client 001.

=head1 VERSION

This documentation refers to Local::EExlsx001 version 1.00

=head1 SYNOPSIS

    use ETL::Pipeline;

    my $etl = ETL::Pipeline->new(
        {
            work_in => 'tmp',
            input   => ['Excel', matching => $in_fn],
            mapping => {name => 'A', age => 'B', utf => 'C'},
            output  => ['+Local::EExlsx001', dbh => $dbh]
        }
    )->process;

=head1 DESCRIPTION

This module takes a file (XLSX) of pseudo employee records and insert
a header, sub header and employee record(s) into the database.

Yes, in theory the type of record (employee, billing info, ...) would be 
inserted into production table of the same type, but here I only need to
be able to inserted the data so the web app option can display what was 
Transform & Loaded by the ETL process. I don't have the data for
Ovid's company for the employment quiz for programmers.
It could be multiple types of records from multiple clients!

In the Transform process you will be able to insert the code to handle
attributes that need be straighten out, munged, before being insert into 
your company's database.

=head1 SUBROUTINES/METHODS

=head2 configure

A default ETL::Pipeline::Output method name called on to insert a 
header for the pseudo employee file being inserted into the database. 

=head2 write_record

A default ETL::Pipeline::Output method name called on to insert a 
sub header and line item for the pseudo employee file being inserted 
into the database. 

=head1 DIAGNOSTICS

=head2 Possible errors

Unable to insert into the database.

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
