# ---------------------------------------------------------------------
package MySQL::Slurp;

    use 5.008 ;
    use Carp;                                         
    use self qw(self);  # We do not import args since this is an attribute
                        # and we get name space clash
    use List::MoreUtils qw(any);
    use File::Path;
    use Moose;
      extends 'MooseX::GlobRef::Object';
      with 'MooseX::Getopt';
    use Mknod;  # function mknod creates FIFO / named pipe      
    use IO::File::flock;    # Lockable IO:File object
    our $VERSION = 0.21;


  # Step 0. Initialize attributes 
    has 'database' => ( is => 'rw', isa => 'Str', required => 1 );
    has 'table'    => ( is => 'rw', isa => 'Str', required => 1 );

    has 'tmp'  => ( 
            is            => 'rw' , 
            isa           => 'Str' , 
            required      => 1 , 
            default       => $ENV{ TMPDIR } || $ENV{ TMP } || '/tmp' || '.' , 
            documentation => "Temporary directory for " . __PACKAGE__  ,
    );                                                                 

    has 'args' => ( 
            is            => 'rw' , 
            isa           => 'ArrayRef' , 
            required      => 0 , 
            default       => sub { [] } ,
            metaclass     => 'NoGetopt' ,
            documentation => 'Flags to pass to mysqlimport.' 
    );

  # In the future this will be deprecated 
  # all will be done through DBI-LOAD DATA ...
    has 'method' => (
            is            => 'rw' ,
            isa           => 'Str' ,
            required      => 1 ,
            default       => 'mysqlimport' ,
            documentation => 'Method to use mysqlimport|LOAD' ,
    );
   

    has 'writer' => (
            is            => 'rw' ,
            isa           => 'IO::File::flock' ,
            required      => 0 ,
            metaclass     => 'NoGetopt' ,
            documentation => 'IO::File::flock filehandle to the pipe' ,
    );
    

    has 'buffer' => ( 
            is            => 'rw' ,
            isa           => 'Int' ,
            required      => 1 ,
            default       => 20 ,
            documentation => 'Records processed before flushing to the file handle ( default: 1)' 
    );

    has '_buffer' => (
            is            => 'rw' ,
            isa           => 'ArrayRef' ,
            required      => 1 ,
            default       => sub { [ ] } ,
            metaclass     => 'NoGetopt' ,
            documentation => 'Write record buffer' ,
    );

    
        
  # ---------------------------------------------------------
  # Internal Attributes
  # ---------------------------------------------------------
  # verbose
  #   Attribute, detect if we are in verbose mode.  We take the flag
  #   from the args attribute.  This allows it to be passed through to
  #   the mysqlimport command as well
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

        if ( self->method eq 'mysqlimport' ) {
            
            my $command = 'mysqlimport --local ' 
                . join( 
                    " ", 
                    @{ self->args }, self->database, self->fifo, "&" 
                );

            print "Executing ... \"$command\" \n" if (self->verbose);
            system( "$command" );

        } elsif ( self->method eq 'LOAD' ) {
        
            # my $command;
            my $command = "mysql --local -e'" .
                 "LOAD DATA LOCAL INFILE \'" . self->fifo . "\'" . 
                 "INTO " . self->database . "." . self->table . "'";
            
            print "Executing ... \"$command\" \n" if (self->verbose);
            system( "$command" );

        } else {

            croak( self->method . " method not supported " );

        }

    }    


# -----------------------------------------------------------
# FILE HANDLE METHODS
#   Connection to the FIFO
# -----------------------------------------------------------
  # open:
  #   Turns object into a MooseX::GlobRef::Object
    sub open {

      # mkfifo  
        self->_mkfifo;  # Create FIFO

      # import 
        self->_import;  # Install reading end of FIFO  

        # self->_install_globref;
        self->_install_writer ;

        return self;
    }


  # Globref is deprecated as of 0.20
  # Writing occurs through the MySQL::Slurp::writer attribute
  # Which is a IO::File::flock object
  # sub _install_globref {

  #     my $self = self;
  #
  #   # open GLobRef connection to FIFO
  #     print STDERR "Opening filehandle '" . self->fifo 
  #       . "' to write to: " . self->database . "." .self->table . "\n" 
  #       if ( self->verbose );
  #
  #     my $hashref = ${ *$self };
  #     &open( $self, ">", self->fifo ) or confess "Cannot open";
  #
  #     return $self;
  #
  # }


    sub _install_writer {

       self->writer( IO::File::flock->new( self->fifo, ">" ) );
       return self->writer;

    }
       
       

    sub close {

        print "Closing filehandle\n" if ( self->verbose );
        # close( $_[0] ) || carp( "Cannot close file handle to " . self->fifo );

        self->flush;
        self->writer->close();
        self->_rmfifo;

    }


    sub flush {

            my $records = scalar @{ self->_buffer };
            
            self->writer->lock_ex;
            print { self->writer } @{ self->_buffer }; 
            self->writer->lock_un; 

            self->_buffer( [] );

            return $records;

    } 

  #
  # Write to writer
  # Buffered 
  # Returns the number of records in the buffer before flush if any.
    sub write {
        
        my $n_records = $#_;

        push( @{ self->_buffer }, @_[1..$#_]); 

      # Flush buffer if it exceeds capacity
        self->flush
          if ( scalar @{ self->_buffer } > self->buffer );

        $n_records;  # return the number of records committed 

    }

  # METHOD:slurp
  #   Slurp from <STDIN>
    sub slurp {

        while( <STDIN> ) {
            self->write( $_ );
            # print {self->writer} $_;
        }
        
    } # END METHOD: slurp 
      

    __PACKAGE__->meta->make_immutable;


__END__

=head1 NAME

MySQL::Slurp - Use PIPEs to import a file into MySQL table.
           
=head1 CAVEAT

  MySQL::Slurp only works on systems that support FIFOs and
  does not support Windows ... yet.

=head1 VERSION

0.20

=head1 SYNOPSIS

    use MySQL::Slurp;

  # NEW OBJECTS 
    my $slurper= MySQL::Slurp->new( 
        database => 'test' , 
        table    => 'table_1' , 
        buffer   => 10000 ,
    );

    $slurper->open;

  # OR,
    my $slurper->new( database => 'test', table => 'table_1' )->open;

  # IMPORT METHODS
    $slurper->slurp();         # slurp from <STDIN>
  
  # RECOMMENDED METHOD TO WRITE TO A TABLE 
  #     implements buffer and locks
    $slurper->write( @records );    

  # WRITE DIRECTLY TO TABLE WITHOUT BUFFER AND LOCKS 
    print {$slurper->writer} "Fred\tFlinstone\n";  
    print { $slurper->{writer} } "Fred\tFlinstone\n";  

    $slurper->close; 

  # In cordinated environents
    $slurper->write( @a );  # In thread 1.
    $slurper->write( @b );  # In thread 2.



=head1 DESCRIPTION

The command-line tool, B<mysqlimport>, is the fastest way to import 
data into MySQL especially using C<--use-threads>.  Unfortunately, 
B<mysqlimport> does not read from <STDIN>.  IN fact, B<mysqlimport> 
only reads from files that have the same name as the target table.  
This is often inconvenient.

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

=item buffer

Maximum number of records that are stored in the buffer before locking
the fifo and flushing to the table.  By default, there is no buffering,
buffer = 1.


=item args      

Options to pass to mysqlimport.  args is an array ref and should appear
exactly as it does in the command line invocation of B<mysqlimport>

=item method

Method to use...presently not implemented, uses mysqlimport.

=back

=head2 open

Opens a connection to the MySQL table through a temporary FIFO.  
Returns a GlobRef that can be directly written to.

=head2 write

Writes arguments to the MySQL database.  Buffering is on by default,
see the L<buffer> attribute.

=head2 close

Closes and removes the pipe and temporary table.

=head2 slurp

Write <STDIN> to the database table.

=head1 THREAD SAFE

MySQL::Slurp is believed to be thread safe if using the 'write' method.
Directly accessing the IO::File pipe is not considered Thread safe.

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
