package MySQL::Slurp::Writer;

# MySQL::Slurp::Writer -  provide a buffer writing file handle
#   Contains everything for the writing
#
# Attributes:
#   buffer      Int
#   _buffer     ArrayRef
#   file        Filename
#   io-file     
#
# Methods: print / write, close, open
#   BUILD
#
#   print
#   flush
#   close
#   lock_ex
#   lock_un
#  
# Or in MySQL::Slurp
# READER ...
#  my $slurper1 = MySQL::Slurp->new( );
#       $slurper->_mkfifo;
#       $slurper->_import;
#       
# READER ...
#   MySQL::Slurp->reader();  # Class based method
#   
# There is no reader method.  There is only a FIFO created on the 
# filesystem
#
# WRITER
#  my $slurper2 = MySQL::Slurp->new( );
#       $slurper2->_install_writer;
#       $slurper2->print( @values );
#
# WRITER
#  my $slurper3 = MySQL::Slurp::Writer->new( filename => .. , buffer => .. );
#     $slurper3->print( @values );
#
# WRITER
# my $writer = $slurper->writer;
#   
#
#   should write test for existence of writer

our $VERSION = 0.04;

use self;
use Fcntl ':flock';
use Moose;
    extends 'IO::File';
    

  # remains in the superclass. 
    has 'filename' => ( 
        is            => 'rw' ,
        isa           => 'Str' ,
        required      => 1 ,            
        documentation => 'Location of file to write to' ,
    );


    has 'buffer' => ( 
            is            => 'rw' ,
            isa           => 'Int' ,
            required      => 1 ,
            default       => 1 ,
            documentation => 'Records processed before flushing to the file handle ( default: 1)' 
    );


  # Actual buffer object
    has '_buffer' => (
            is            => 'rw' ,
            isa           => 'ArrayRef' ,
            required      => 1 ,
            default       => sub { [ ] } ,
#           metaclass     => 'NoGetopt' ,
            documentation => 'Write record buffer' ,
    );


    has 'iofile' => (
            is            => 'ro' ,
            isa           => 'IO::File' ,
            required      => 0 ,
#           metaclass     => 'NoGetopt' ,
            documentation => 'IO::File object' ,
    );
    


# ---------------------------------------------------------------------
# INSTANTIATION
# ---------------------------------------------------------------------

    sub BUILD { 

      # Create a iofile handle
        self->{iofile} = IO::File->new( self->filename, ">" ) ;

    }
        

# ---------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------

  # Buffered 
  # Returns the number of records in the buffer before flush if any.
    sub print {
        
        my $n_records = $#_;

        push( @{ self->_buffer }, @_[1..$#_]); 

      # Flush buffer if it exceeds capacity
        self->flush
          if ( scalar @{ self->_buffer } > self->buffer );

        $n_records;  # return the number of records committed 

    }       


  # Flush buffer to FIFO
    sub flush {

            my $records = scalar @{ self->_buffer };
            
            self->lock_ex;
            print { self->iofile } @{ self->_buffer }; 
            self->lock_un; 

            self->_buffer( [] );

            return $records;

    }


    sub lock_ex {

        flock( self->iofile, LOCK_EX );

    }

    sub lock_un {

        flock( self->iofile, LOCK_UN );

    }

    sub close { 

        self->flush;
        self->iofile->close;

    }

# ---------------------------------------------------------------------
# EVENTS
# ---------------------------------------------------------------------



    __PACKAGE__->meta->make_immutable;

1;


# ---------------------------------------------------------------------
__END__


=head1 NAME

MySQL::Slurp::Writer - Adds buffering / locking writing to MySQL::Slurp 


=head1 SYNOPSIS

    my $writer = MySQL::Slurp::Writer->new( ... );

    $writer->print( "records\tto\tprint\n" );


=head1 DESCRIPTION

This module wraps L<IO::File> to provide a thread-safe method for 
writing to a file handles.  The method is simple ... writing is 
buffered; the file handle is locked; the output is written to the file
handle, the lock is released.    

=head1 METHODS

=head2 new

Create a new MySQL::Slurp::Writer object

=over

=item buffer

The size of the buffer.  The default is 1 record, i.e. no buffering.

=item filename

The filename of the IO::File object

=back

=head2 print

Write arguments to the buffer and if the buffer is full, commit to the
file handle

=head2 flush

Flush the buffer

=head2 close

Closes the writing file handle

=head2 lock_ex 

Block until an exclusive lock can be made on the file handle

=head2 lock_un

Release the lock

=head1 TODO

- item Generalize to object independent of MySQL::Slurp

=head1 SEE ALSO

L<MySQL::Slurp>, L<IO::File>, L<Moose>


=head1 AUTHOR

Christopher Brown, E<lt>ctbrown@cpan.org<gt>

L<http://www.opendatagroup.com>


=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Open Data

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut              


