package MySQL::Slurp;

=head1 NAME

MySQL::Slurp - Use PIPEs to write directly to a MySQL table
           
=head1 CAVEAT

  MySQL::Slurp only works on systems that support FIFOs and
  does not support Windows ... yet.

=cut

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

=head1 VERSION

0.27

=cut

    our $VERSION = 0.27;

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
    my $slurper = MySQL::Slurp->new( 
        database => 'test', 
        table => 'table_1' 
    )->open;


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

MySQL::Slurp provides methods for writing directly to a MySQL table
using a convenient interface. This module creates a writable FIFO and
uses C<mysqlimport> or C<LOAD DATA INFILE> to load whatever is written
to that FIFO.  This is the fastest method for importing data into a 
MySQL table.  This module makes it easy.  The user needs only C<open>,
C<write>, and C<close> a MySQL::Slurp object.

This module also provides a C<slurp> method for reaing directly from 
<STDIN> and writing to the table.  This allows you to do tasks such 
as the following: 

    cat data.tsv | perl myscript.pl   

This is very handy for large ETL jobs.

Unike using L<DBI> for trapping errors, catching errors with 
mysqlimport can be troublesome with inconsitent data.  It is 
recommended that you check you data before writing to the MySQL::Slurp
handle or use a suitable L<DBI> method.  

The module also implements buffering and locking using 
L<MySQL::Slurp::Writer>.  This allows for multi-process and multi-
threading.


=head1 METHODS

=head2 new 

Creates a new MySQL::Slurp object

=over 

=item database (required)  

name of the MySQL database containing the target table. 

=cut 

    has 'database' => ( 
            is             => 'rw', 
            isa            => 'Str', 
            required       => 1,  
            documentation  => 'Target database' ,
    );


=item table (required)

Name of MySQL table to write to.   

=cut 

    has 'table'    => ( 
            is            => 'rw', 
            isa           => 'Str', 
            required      => 1 ,
            documentation => 'Target table' ,
    );


=item tmp  

default: $ENV{TMPDIR} || $ENV{TMP} || /tmp || . ( present directory )

The (name of the) temporary directory in which the FIFO/pipe is created.

=cut 

    has 'tmp'  => ( 
            is            => 'rw' , 
            isa           => 'Str' , 
            required      => 1 , 
            default       => $ENV{ TMPDIR } || $ENV{ TMP } || '/tmp' || '.' , 
            documentation => "Temporary directory for " . __PACKAGE__  ,
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


=item buffer  

default: 1  ( buffer one line, i.e. no buffering )

Maximum number of records that are stored in the buffer before locking
the fifo and flushing to the MySQL table.  By default, there is no 
buffering,  Buffer = 1.  This attribute is used by L<MySQL::Slurp::Writer>

There is no checking for memory limits.  The user is responsible for 
using a sensible value.

=cut 

    has 'buffer' => ( 
            is            => 'rw' ,
            isa           => 'Int' ,
            required      => 1 ,
            default       => 1 ,
            documentation => 'Records processed before flushing to the file handle ( default: 1)' 
    );


=item method 

default: dbi 

Method to use for importing.  Supports c<mysqlimport>, c<mysql> and 
c<dbi> for mysqlimport, mysql and dbi loading methods, respectively.

c<dbi> is the default method.  This method uses the DBI module and is the
most portable.

C<mysql> uses the C<mysql> command line application.

C<mysqlimport> uses the mysqlimport appication.  This is faster than the 
DBI method.  It reads settings from C<~/.my.cnf>.  

=cut 

    has 'method' => (
            is            => 'rw' ,
            isa           => 'Str' ,
            required      => 1 ,
            default       => 'dbi' ,
            documentation => 'Method to use mysqlimport|mysql|dbi|dbi-delayed' ,
    );
   

=item args      

Options to pass to mysqlimport.  C<args> is an array ref and should appear
exactly as it does in the command line invocation of mysqlimport.  Applies
to C<mysqlimport> method only

=cut 

    has 'args' => ( 
            is            => 'rw' , 
            isa           => 'ArrayRef' , 
            required      => 0 , 
            default       => sub { [] } ,
            metaclass     => 'NoGetopt' ,
            documentation => 'Flags to pass to mysqlimport.' 
    );


=item verbose 

Whether to display verbose output 

=cut 

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


=item force 

Continue even if errors are encountered 

=cut

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


=back 

=head2 open

Opens a connection to the MySQL table through a temporary FIFO.  
Returns a GlobRef that can be directly written to.  This calls
internal methods L<_mkfifo>, <_import>, <_install_writer>,  After the
C<MySQL::Slurp> object is open, one can print directly to the table.

=cut 


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


=head2 print

Writes arguments directly to the MySQL database.  Buffering is off by default,
see the L<buffer> attribute.

=cut 

    sub print {

        self->writer->print( @_[1..$#_] );

    } 


=head2 slurp

Read from <STDIN> and write to the database  table.

=cut

    sub slurp {

        while( <STDIN> ) {
            self->print( $_ );
        }
        
    } # END METHOD: slurp 


=head2 close

Closes and removes the pipe and temporary table.  
Calls C<MySQL::Slurp::Writer::close> and L<_rmfifo>.

=cut 

    sub close {

        print "Closing filehandle\n" if ( self->verbose );

        # self->writer->flush;
        self->writer->close();
        self->_rmfifo;

    }


=head1 INTERNAL SLOTS

=head2 writer

The slow holding the L<MySQL::Slurp::Writer> used for buffering the 
writing and thread-safe.

=cut

    has 'writer' => (
            is            => 'rw' ,
            isa           => 'MySQL::Slurp::Writer' ,
            required      => 0 ,
            metaclass     => 'NoGetopt' ,
            documentation => 'IO::File::flock filehandle to the pipe' ,
    );
    

=head2 dbh

The database handle to the target table.  The handle is created using
the defaults in your C<~/.my.cnf> file.  Compression is used and query 
is streamed, C<mysql_use_result=1>.

=cut

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



  # Since FIFO is required to match the table name, and it is
  # dependent on the other features, we do not make it an 
  # attribute but install it as a method ... lazy

=head2 fifo

The fifo used for import.  This is simply the path.  It is set to the 
name of the C<tmp> directory and the name of the C<table>.  

=cut

    has fifo => ( 
        is            => 'ro'  , 
        isa           => 'Str' ,
        required      => 1 ,
        lazy          => 1 ,
        default       => sub { self->dir  . "/" . self->table . ".txt" } ,
        documentation => 'Location of the fifo' ,
        metaclass     => 'NoGetopt' ,  
    );


# ---------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------

=head1 INTERNAL METHODS

Do not use these methods directly.

=head2 _mkfifo

Creates the FIFO at C<[tmp]/mysqlslurp/[table].txt>.  This will die if 
a pipe, file, directory exists with the same descriptor.

=cut 

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

    } 


=head2 _rmfifo

Removes the FIFO.  Used in cleaning up after the upload.

=cut

    sub _rmfifo {
        
        print  "Removing FIFO ... " . self->fifo . "\n" if ( self->verbose ); 

        if ( -p self->fifo ) {
            unlink self->fifo or warn( "Cannot remove fifo " . self->fifo );
        } 

        if ( -d self->dir ) {   # and  ! self->dir_exists ) {
            rmtree( self->dir );
        }

    }
        

=head2 _import

This is the heart of C<MySQL::Slurp>, reading from the fifo and writing
from the table.

=cut 

    sub _import {                                       

        my $sql = 
           "LOAD DATA LOCAL INFILE \'" . self->fifo . "\' " . 
           "INTO TABLE " . self->database . "." . self->table ; 

      # METHOD: MYSQLIMPORT 
        if ( self->method eq 'mysqlimport' ) {
            
            my $command = 'mysqlimport --local ' 
                . join( 
                    " ", 
                    @{ self->args }, self->database, self->fifo, "&" 
                );

            print "Executing ... \"$command\" \n" if (self->verbose);
            system( "$command" );

      # METHOD: MYSQL
        } elsif ( self->method eq 'mysql' ) {

            my $command = "mysql --local-infile -e\"$sql\"" ;

            print "Executing ... \"$command\" \n" if (self->verbose);

            system( "$command &" ); # Command must be placed in background

      # METHOD: dbi 
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


=head2 _install_writer

=cut

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
       


=head2 _write 

Print to L<MySQL::Slurp::Writer> object located in C<self->writer>.

=cut


    sub _write {
    
        print { self->writer } @_[1..$#_] ;
    
    }


      

    __PACKAGE__->meta->make_immutable;


__END__


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
