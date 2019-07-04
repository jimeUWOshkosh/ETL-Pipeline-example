package Up;
use Mojo::Base 'Mojolicious';
use utf8;
use Carp 'croak';
use lib 'lib';
use Try::Tiny;
use Up::Schema;
our $VERSION = '1.00';

# This method will run once at server start
sub startup {
    my $self = shift;

    # Load configuration from hash returned by "up.conf"
    my $config = $self->plugin('Config');

    # Switched to 'ep' to get csrf_field/csrf_token to work
    # Using Template Toolkit
    #  $self->plugin('tt_renderer');
    #  $self->renderer->default_handler('tt');

    # Router
    my $r = $self->routes;

    # menu
    $r->get(q{/})->to( controller => 'upload', action => 'menu' );
    $r->post('/uploadmenu')->to( controller => 'upload', action => 'menu' );

    # find out what was selected and redirect them
    $r->post('UploadMenuChose')->to( controller => 'upload', action => 'MenuChose' );

    # Choose file(s) to upload and process them
    $r->get('FileUpload')->to( controller => 'upload', action => 'FileUpload' );
    $r->post('ProcessFiles')->to( controller => 'upload', action => 'ProcessFiles' );

    # Show the big list of files processed and let them choose a file to view
    $r->get('biglist')->to( controller => 'upload', action => 'biglist' );
    $r->post('displaydataset')->to( controller => 'upload', action => 'displaydataset' );

    my $schema;
    try {
        $schema = Up::Schema->connect(
            $config->{DSN},      $config->{USER},
            $config->{PASSWORD}, $config->{DBEXTRA},
        );
    }
    catch {
        croak 'RDBMS Issue:';
    };

    # DBIX::Class::Schema helper
    $self->helper( db => sub { return $schema; } );

    # trick to get $schema to Mojolicious Models
    ( ref $self )->attr( db => sub { return $schema } );

    #  $self->app->log->debug("exiting Startup");
    return;
}
1;

=head1 NAME

Up - Upload web application to upload file(s) of pseudo
     employee records to a ETL system which places
     data into a database so that they can be displayed
     to the user after the upload is completed.

=head1 VERSION


This documentation refers to Up version 1.00

=head1 SYNOPSIS

   The Mojolicious web application is execute in an Mojolicious
   start up script by calling

   Mojolicious::Commands->start_app('Up');


=head1 DESCRIPTION

A full description of the module and its features.

May include numerous subsections (i.e., =head2, =head3, etc.).

=head1 SUBROUTINES/METHODS

=head2 startup

    Default Mojolicous named subroutine

    Reads in configuration file
    Defines routes of the web app
    Connect to the database
    Create a couple of helpers to be able to pass database handle

=head1 DIAGNOSTICS

=head2 Possible errors

  Failed to find configuration file
  Failed to connect to database
  Bad Controller name and its methods
  Bad templates name and contents
  Bad template layouts name and contents

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
