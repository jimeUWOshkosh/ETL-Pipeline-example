package Local::DelimitedTextUnicode;
use Moose;

use 5.014000;
use warnings;

use Carp;
use Text::CSV;
use Text::CSV::Unicode;
use Carp 'croak';

our $VERSION = '2.00';
my $rc;

sub BUILD {
    my $self      = shift;
    my $arguments = shift;

    my %options;
    while ( my ( $key, $value ) = each %$arguments ) {
        $options{$key} = $value unless $self->meta->has_attribute($key);
    }

    $self->csv( Text::CSV::Unicode->new( \%options ) );
    return;
}

sub get {
    my ( $self, $index ) = @_;
    return undef unless $index =~ m/^\d+$/;    ## no critic
    return $self->_get_value($index);
}

sub next_record {
    my ($self) = @_;

    my $fields = $self->csv->getline( $self->handle );
    if ( defined $fields ) {
        $self->record($fields);
        return 1;
    }
    else {
        return 0 if $self->csv->eof;
        my ( $code, $message, $position ) = $self->csv->error_diag;
        croak "Error $code: $message at character $position";
    }
    return;
}

sub get_column_names {
    my ($self) = @_;

    $self->next_record;
    $self->add_column($_) foreach ( $self->fields );
    return;
}

sub configure {
    my ($self) = @_;

    $self->handle( $self->file->openr() );
    croak sprintf 'Unable to open "%s" for reading', $self->file->stringify
        unless defined $self->handle;
    return;
}

sub finish { $rc = close shift->handle; return; }

has 'record' => (
    handles => {
        fields           => 'elements',
        _get_value       => 'get',
        number_of_fields => 'count',
    },
    is     => 'rw',
    isa    => 'ArrayRef[Any]',
    traits => [qw/Array/],
);

has 'csv' => (
    is  => 'rw',
    isa => 'Text::CSV',
);

has 'handle' => (
    is  => 'rw',
    isa => 'Maybe[FileHandle]',
);

with 'ETL::Pipeline::Input::File';
with 'ETL::Pipeline::Input::Tabular';
with 'ETL::Pipeline::Input';


no Moose;
__PACKAGE__->meta->make_immutable;
1;

=head1 NAME

ETL::Pipeline::Input::DelimitedTextUnicode Input source for CSV, tab, or pipe
delimited files

=head1 VERSION

This documentation refers to <Module::Name> version 2.01

=head1 SYNOPSIS

  use ETL::Pipeline;
  ETL::Pipeline->new( {
    input   => ['DelimitedTextUnicode', matching => qr/\.csv$/i],
  } )->process;

=head1 DESCRIPTION

ETL::Pipeline::Input::DelimitedTextUnicode defines an input source for reading 
CSV (comma seperated variable), tab delimited, or pipe delimited files. It
uses Text::CSV::Unicode for parsing.

=head1 METHODS & ATTRIBUTES

=head2 Arguments for ETL::Pipeline/input

ETL::Pipeline::Input::DelimitedTextUnicode implements ETL::Pipeline::Input::File
and ETL::Pipeline::Input::TabularFile. It supports all of the attributes
from these roles.

In addition, ETL::Pipeline::Input::DelimitedTextUnicode makes available all of the
options for Text::CSV::Unicode. 

  # Pipe delimited, allowing embedded new lines.
  $etl->input( 'DelimitedTextUnicode', 
    matching => qr/\.dat$/i, 
    sep_char => '|', 
    binary => 1
  );

=head1 SUBROUTINES/METHODS

=head2 Called from ETL::Pipeline/process

=head3 get

B<get> retrieves one field from the current record. B<get> accepts one
parameter. That parameter can be an index number, a column name, or a regular
expression to match against column names.

  $etl->get( 0 );
  $etl->get( 'First' );
  $etl->get( qr/\bfirst\b/i );

=head3 next_record

Read one record from the file for processing. B<next_record> returns a boolean.
I<True> means success. I<False> means it reached the end of the file.

  while ($input->next_record) {
    ...
  }

=head3 get_column_names

B<get_column_names> reads the field names from the first row in the file.
L</get> can match field names using regular expressions.

=head3 configure

B<configure> opens the file for reading. It takes care of loading the column
names, if your file has them.

=head3 finish

B<finish> closes the file.

=head2 Other Methods & Attributes

=head3 record

ETL::Pipeline::Input::DelimitedTextUnicode stores each record as a list of fields.
The field name corresponds with the file order of the field, starting at zero.
This attribute holds the current record.

=head3 fields

Returns a list of fields from the current record. It dereferences L</record>.

=head3 number_of_fields

This method returns the number of fields in the current record.

=head3 csv

B<csv> holds a Text::CSV::Unicode object for reading the file. You can set options
for Text::CSV::Unicode in the ETL::Pipeline/input command. 

=head3 handle

The Perl file handle for reading data. Text::CSV::Unicode operates on a handle. 
next_record needs the handle.

=head1 SEE ALSO

ETL::Pipeline, ETL::Pipeline::Input, ETL::Pipeline::Input::File,
ETL::Pipeline::Input::Tabular

=head1 DIAGNOSTICS

No to report at the current time.

=head1 CONFIGURATION AND ENVIRONMENT

None to report at the current time.

=head1 DEPENDENCIES

None to report at the current time.

=head1 INCOMPATIBILITIES

None to report at the current time.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.

The original author of ETL::Pipeline project at the 
current time June-2019 is not maintaining the CPAN 
module and not responding to issues (via rt.cpan.org or github).

=head1 AUTHOR

Robert Wohlfarth <robert.j.wohlfarth@vumc.org>
Modified for UTF8 by Jim Edwards July-2019

=head1 LICENSE

Copyright 2016 (c) Vanderbilt University Medical Center

This program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

