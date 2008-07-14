# ---------------------------------------------------------------------
package MySQL::Slurp;

    use 5.008 ;
    use Carp;                                         
    use Perl6::Say;
    use self qw(self args);
    use List::MoreUtils qw(any);
    use File::Path;
    use Moose;
      extends 'MooseX::GlobRef::Object';
    use Mknod;  # function mknod creates FIFO / named pipe      
    our $VERSION = 0.15;


  # Step 0. Initialize attributes 
    has 'database' => ( is => 'rw', isa => 'Str', required => 1 );
    has 'table'    => ( is => 'rw', isa => 'Str', required => 1 );
    has 'tmp'      => ( 
            is       => 'rw' , 
            isa      => 'Str' , 
            required => 1 , 
            default  => $ENV{ TMPDIR } || $ENV{ TMP } || '/tmp' || '.' , 
          # documentation => "Temporary directory for " . __PACKAGE__  ,
    );                                                                 

    has 'args' => ( 
            is       => 'rw' , 
            isa      => 'ArrayRef' , 
            required => 0 , 
            default  => sub { [] } ,
            documentation => 'Flags to pass to mysqlimport.' 
    );


        
  # ---------------------------------------------------------
  # Internal Attributes
  # ---------------------------------------------------------
  # verbose
  #   Attribute, detect if we are in verbose mode.  We take the flag
  #   from the args attribute
    sub verbose {
        if ( any { $_ =~ /[^\w] (-v|--verbose) [ \w$ ]/x } @{ self->args } ) {
            return 1 ;
        } else {
            return 0;
        }
    }

  # force
  #   Attribute.  Indicates if we are in force mode.
    sub force {
        if ( any { $_ =~ /[^\w] (-f|--force)   [ \w$ ]/x } @{ self->args } ) {
            return 1 ;
        } else {
            return 0 ;
        }
    }



# -----------------------------------------------------------
# FIFO Methods
# -----------------------------------------------------------

  # Since FIFO is required to match the table name, and it is
  # dependent on the other features, we do not make it an 
  # attribute.
    sub fifo {
        return( self->dir  . "/" . self->table . ".txt" ) ;
    }

    sub dir {
        return( self->tmp . "/mysqlslurp/" . self->database );
    }


  # Create FIFO and if necessary a directory in self->tmp
    sub _mkfifo {

        print "Making FIFO ... " . self->fifo . "\n" if ( self->verbose );

        unlink( self->fifo ) if ( -p self->fifo and self->force );

        croak( "A FIFO already exists for that table.  Delete with 'rm -f "
            . self->fifo . "' before proceeding\n" ) if ( -e self->fifo );

       # MAKE FIFO
         if ( ! -e self->dir ) {
            mkpath( self->dir, { mode => 0722 } )  
                or croak( "Cannot make directory ... " . self->dir );
         } 

         mknod( self->fifo , S_IFIFO|0644 ) 
            or croak( "Cannot make FIFO" );
         # carp( "Cannot create FIFO " . self->fifo . "\n" ) if ( ! -p  self->fifo );

    } 

  # Remove FIFO and if it didn't previously exist the directory within tmp.
    sub _rmfifo {
        
        print  "Removing FIFO ... " . self->fifo . "\n" if ( self->verbose ); 

        if ( -p self->fifo ) {
            unlink self->fifo or warn( "Cannot remove fifo " . self->fifo );
        } 

        if ( -d self->dir ) {   # and  ! self->dir_exists ) {
            rmtree( self->dir );
        }

    }
        
# -----------------------------------------------------------
# MySQL Import wrapper
#   Executes a system command around mysqlimport
# -----------------------------------------------------------
    sub _import {                                       

         my $command = 'mysqlimport --local ' 
            . join( 
                " ", 
                @{ self->args }, self->database, self->fifo, "&" 
              );

         print "Executing ... \"$command\" \n" if (self->verbose);
         system( "$command" );

    }    


# -----------------------------------------------------------
# FILE HANDLE METHODS
#   Connection to the FIFO
# -----------------------------------------------------------
  # open:
  #   Turns object into a MooseX::GlobRef::Object
    sub open {

        my $self = self;

      # mkfifo
        self->_mkfifo;

      # import 
        self->_import;

      # open GLobRef connection to FIFO
        print STDERR "Opening filehandle '" . self->fifo 
          . "' to write to: " . self->database . "." .self->table . "\n" 
          if ( self->verbose );

        my $hashref = ${ *$self };
        open( $self, ">", self->fifo ) or confess "Cannot open";

        return $self;

    }


    sub close {

        print "Closing filehandle\n" if ( self->verbose );
        close( $_[0] ) || carp( "Cannot close file handle to " . self->fifo );

        self->_rmfifo;

    }



  # METHOD:slurp
  #   Slurp from <STDIN>
    sub slurp {

        while( <STDIN> ) {
            print {self} $_;
        }
        
    } # END METHOD: slurp 
      

    __PACKAGE__->meta->make_immutable;


__END__

=head1 NAME

MySQL::Slurp - Use PIPEs to import a file into MySQL table.
           
=head1 CAVEAT

  MySQL::Slurp only works on systems that support FIFOs and
  does not support Windows ... yet.

=head1 SYNOPSIS

    use MySQL::Slurp;

  # Object method
    my $slurper= MySQL::Slurp->new( database => 'test', table => 'table_1' );
    $slurper->open;

    $slurper->slurp();         # slurp from <STDIN>
    
    print $slurper "Fred\tFlinstone\n"; # Print directly to table
    print $slurper "Barney\tRubble\n"; 

    $slurper->close;


=head1 DESCRIPTION

The command-line tool, B<mysqlimport>, is the fastest way to import 
data into MySQL especially using C<--use-threads>.  It is faster than
c<LOAD DATA INFILE> especially when use the C<--use-threads> option.  
Unfortunately, B<mysqlimport> does not read from <STDIN>.  IN fact, 
B<mysqlimport> only reads from files that have the same name as the 
target table.  This is often inconvenient.

B<MySQL::Slurp> has the speed of B<mysqlimport> but permits loading
from <STDIN> or provides a GlobRef file handle for writing directly to a
MySQL table.  This is very handy for large ETL jobs.

This module simply wraps and B<mysqlimport> and creates the necessary
FIFO.  As such, catching (data) errors is relegated to B<mysqlimport>.
Unike using L<DBI> for trapping errors, catching errors with 
B<mysqlimport> can be troublesome with inconsitent data.  It is 
recommended that you check you data before writing to the B<MySQL::Slurp>
handle or use a suitable L<DBI> method.
inconsistent.


=head1 METHODS

=head2 new 

Creates a new MySQL::Slurp object.

=over 

=item database  

name of database (required)

=item table 

Name of table to import (required)

=item tmp       

Name of temporary directory (optional)

=item args      

Options to pass to mysqlimport.  args is an array ref and should appear
exactly as it does in the command line invocation of B<mysqlimport>

=back

=head2 open

Opens a connection to the MySQL table through a temporary FIFO.  
Returns a GlobRef that can be directly written to.

=head2 close

Closes and removes the pipe and temporary table.

=head2 slurp

Write <STDIN> to the database table.


=head1 EXPORT

None.

=head1 TODO

- use MooseX::Attribute::Defaults::GNU for object attributes

- remove reliance on installation of mysqlimport, by XS wrapping the C libraries.

- Better error catching than mysqlimport

- create a version to run on windows with named pipes(?)

- alias attribute tmp as temp.

- allow options for slurp to change the MySQL::Slurp object's attributes


=head1 SEE ALSO

MySQL::Slurp relies on the L<Moose> metaobject package. 

mysqlimport at L<http://mysql.com>, currently 
L<http://dev.mysql.com/doc/refman/5.1/en/mysqlimport.html>

=head1 AUTHOR

Christopher Brown, E<lt>ctbrown@cpan.org<gt>

L<http://www.opendatagroup.com>


=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Open Data

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.


=cut
