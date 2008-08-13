# ---------------------------------------------------------------------
package MySQL::Slurp;

    our $VERSION = 0.24;

    use 5.008 ;
    use Carp;                                         
    use self qw(self);  # We do not import args since this is an attribute
                        # and we get name space clash
    use List::MoreUtils qw(any);
    use File::Path;
    use Moose;
        with 'MooseX::Getopt';          # For NoGetopt tags

    use Mknod;  # function mknod creates FIFO / named pipe      
    use MySQL::Slurp::Writer;

    use DBI;
    

# ---------------------------------------------------------------------
# ATTRIBUTES
# ---------------------------------------------------------------------

  # Step 0. Initialize attributes 
    has 'database' => ( 
            is             => 'rw', 
            isa            => 'Str', 
            required       => 1,  
            documentation  => 'Target database' ,
    );


    has 'table'    => ( 
            is            => 'rw', 
            isa           => 'Str', 
            required      => 1 ,
            documentation => 'Target table' ,
    );


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
            default       => 'dbi' ,
            documentation => 'Method to use mysqlimport|mysql|dbi|dbi-delayed' ,
    );
   
# Moved into MySQL::Slurp::Writer roles
    has 'writer' => (
            is            => 'rw' ,
            isa           => 'MySQL::Slurp::Writer' ,
            required      => 0 ,
            metaclass     => 'NoGetopt' ,
            documentation => 'IO::File::flock filehandle to the pipe' ,
    );
    

    has 'buffer' => ( 
            is            => 'rw' ,
            isa           => 'Int' ,
            required      => 1 ,
            default       => 1 ,
            documentation => 'Records processed before flushing to the file handle ( default: 1)' 
    );



    has 'dbh' => (
            is           => 'ro' ,
            isa          => 'DBI::db' ,
            required     => 0 ,
            lazy         => 1 ,
            default      => sub {

                my $dsn = join( ';', 
                        "DBI:mysql:database=" . self->database ,
                        # "host=" . self->host ,
                        "mysql_read_default_file=~/.my.cnf" ,
                        "mysql_compression=1" ,
                        "mysql_use_result=1"
                      );

                return DBI->connect( $dsn );

            } ,
            documentation => 'Database handle' ,
            metaclass     => 'NoGetopt' ,  
    ); 


# ---------------------------------------------------------
# Internal Attributes
# ---------------------------------------------------------
  # verbose, force
  #   Attribute, detect if we are in verbose mode.  We take the flag
  #   from the args attribute.  This allows it to be passed through to
  #   the mysqlimport command as well

    has verbose => (
        is            => 'ro' ,
        isa           => 'Bool' ,
        required      => 1 ,
        lazy          => 1 ,
        default       => 
            sub { 
                if ( 
                    any { $_ =~ /[^\w] (-v|--verbose) [ \w$ ]/x } @{ self->args }                  ) {
                    return 1 ;
                } else {
                    return 0 ;
                }    
            } ,
        documentation => 'Force writing on error' ,
        metaclass     => 'NoGetopt' ,  
    );      


    has force => (
        is            => 'ro' ,
        isa           => 'Bool' ,
        required      => 1 ,
        lazy          => 1 ,
        default       => 
            sub { 
                if ( 
                    any { $_ =~ /[^\w] (-f|--force)   [ \w$ ]/x } @{ self->args }                  ) {
                    return 1 ;
                } else {
                    return 0 ;
                }    
            } ,
        documentation => 'Force writing on error' ,
        metaclass     => 'NoGetopt' ,  
    );

# -----------------------------------------------------------
# FIFO Attributes
# -----------------------------------------------------------

  # Since FIFO is required to match the table name, and it is
  # dependent on the other features, we do not make it an 
  # attribute but install it as a method ... lazy

    has fifo => ( 
        is            => 'ro'  , 
        isa           => 'Str' ,
        required      => 1 ,
        lazy          => 1 ,
        default       => sub { self->dir  . "/" . self->table . ".txt" } ,
        documentation => 'Location of the fifo' ,
        metaclass     => 'NoGetopt' ,  
    );


    has dir => (
        is             => 'ro' ,
        isa            => 'Str' ,
        required       => 1 ,
        lazy           => 1 ,
        default        => sub {  self->tmp . "/mysqlslurp/" . self->database } , 
        documentation  => 'Location of temporary mysqlslurp directory' ,
        metaclass      => 'NoGetopt' ,  
    );



# ---------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------

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
#   Creates the import method
# -----------------------------------------------------------
    sub _import {                                       

        my $sql = 
           "LOAD DATA LOCAL INFILE \'" . self->fifo . "\' " . 
           "INTO TABLE " . self->database . "." . self->table ; 

        if ( self->method eq 'mysqlimport' ) {
            
            my $command = 'mysqlimport --local ' 
                . join( 
                    " ", 
                    @{ self->args }, self->database, self->fifo, "&" 
                );

            print "Executing ... \"$command\" \n" if (self->verbose);
            system( "$command" );

        } elsif ( self->method eq 'mysql' ) {

            my $command = "mysql --local-infile -e\"$sql\"" ;

            print "Executing ... \"$command\" \n" if (self->verbose);

            system( "$command &" ); # Command must be placed in background

        } elsif ( self->method eq 'dbi' ) {

            print "Forking $sql \n" if (self->verbose);
            my $pid = fork;
            if ( $pid ) {
              # Parent: Do nothing but continue 
            } elsif ( defined $pid ) { 
              # Execute statement in child
                self->dbh->do( $sql ); 
                exit 0;
            } 

        } elsif ( self->method eq 'dbi-delayed' ) {

            croak( "dbi-delayed method not yet available" );

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


    sub _install_writer {

       # self->writer( IO::File->new( self->fifo, ">" ) );
        self->writer( 
          MySQL::Slurp::Writer->new( 
            filename => self->fifo ,
            buffer   => self->buffer ,
          )
        );

       return self->writer; # important for some reason.

    }
       

    sub close {

        print "Closing filehandle\n" if ( self->verbose );

        # self->writer->flush;
        self->writer->close();
        self->_rmfifo;

    }


    sub print {

        self->writer->print( @_[1..$#_] );

    } 


    sub _write_ {
    
        print { self->writer } @_[1..$#_] ;
    
    }


  # METHOD:slurp
  #   Slurp from <STDIN>
    sub slurp {

        while( <STDIN> ) {
            self->print( $_ );
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

0.23

=head1 SYNOPSIS

    use MySQL::Slurp;

  # NEW OBJECTS 
    my $slurper= MySQL::Slurp->new( 
        database => 'test' , 
        table    => 'table_1' , 
        buffer   => 10000 ,
        args     => []    ,
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
    $slurper->print( "Fred\tFlinstone\n" );
    print { $slurper->{writer} } "Fred\tFlinstone\n";  

    $slurper->close; 


  # In coordinated environents
    my $slurper1 = MySQL::Slurp::Writer->new( ... );
    my $slurper2 = MySQL::Slurp::Writer->new( ... );

    $slurper1->write( @a );  # In thread 1.
    $slurper2->write( @b );  # In thread 2.



=head1 DESCRIPTION

MySQL::Slurp slurps data directly into a MySQL table.  This is the 
fastest way to import data into MySQL.

By itself mysqlimport does not allow reading from C<STDIN>.  IN fact, 
mysqlimport only reads from files that have the same name as the target
table.  This is very often inconvenient. 

This module provides a library and tool that wraps mysqlimport and the
mkfifo to allow piping data directly into MySQL tables.  It allows such 
things as:

  cat file | perl myscript.pl 

This is very handy for large ETL jobs.

Unike using L<DBI> for trapping errors, catching errors with 
mysqlimport can be troublesome with inconsitent data.  It is 
recommended that you check you data before writing to the MySQL::Slurp
handle or use a suitable L<DBI> method.  


=head1 METHODS

=head2 new 

Creates a new MySQL::Slurp object

=over 

=item database  

name of database (required)

=item table 

Name of table to import (required)

=item tmp       

Name of temporary directory (optional)

=item buffer  ( default: 1 )

Maximum number of records that are stored in the buffer before locking
the fifo and flushing to the table.  By default, there is no buffering,
buffer = 1.

=item method ( default: mysqlimport )

Method to use for importing.  Supports c<mysqlimport>, c<mysql> and 
c<dbi> for mysqlimport, mysql and dbi loading methods, respectively.

=item args      

Options to pass to mysqlimport.  args is an array ref and should appear
exactly as it does in the command line invocation of mysqlimport.

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


=head1 TODO

- use MooseX::Attribute::Defaults::GNU for object attributes

- remove reliance on installation of mysqlimport, by XS wrapping the C 
libraries.

- create a version to run on windows with named pipes(?)

- create method for INSERT DELAYED 




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
