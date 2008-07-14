# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MySQL-Slurp.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More tests => 13;
BEGIN { use_ok('MySQL::Slurp') };

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

    if ( $^O =~ /Win/i ) {
        BAIL_OUT( "MySQL::Slurp does not work on windows ... yet\n" );
    }


  
    my $load = MySQL::Slurp->new( 
        database => 'test', 
        table => 'mysqlslurp', 
        args => ["--force" ] 
    );

    isa_ok( $load, 'MySQL::Slurp' );

  # Attributes 
    diag( "Testing Attributes" );
    ok( $load->database eq 'test', 'Attribute: database' );
    ok( $load->table    eq 'mysqlslurp' , 'Attribute: table' );
    ok( -e $load->tmp            , 'Temporary directory exists' );
    ok( $load->force    == 1     , 'Hidden attribute recognized' );
    ok( $load->dir      
        eq ( $load->tmp . '/mysqlslurp/' . $load->database ) ,
        'FIFO directory' 
    );

    # ok( $load->fifo     eq $load->  
    # print $load->fifo;
              
              
  # Methods 
  if  ( ! $ENV{mysqlslurp} ) {
    diag( "In order oo run live tests set the following env variables:" );
    diag( "\tmysqlslurp=1 to indicate to run the tests and optionally, " );
    diag( "\tmysqlslurp_user" );
    diag( "\tmysqlslurp_pass" );
    diag( "\tmysqlslurp_host" );
    diag( "For a user with write create permissions on the test database.");
    diag( "A table mysqlslurp will be created for testing purposes and" );
    diag( "dropped when done." );
 }

    SKIP: {                    
        
        skip "Skipping live tests", 6 if ! $ENV{mysqlslurp} ;
        diag( "Testing Methods" );

        my $command =  "mysql";
           $command .= " -u$ENV{mysqlslurp_user}" if ( $ENV{mysqlslurp_user} );
           $command .= " -p$ENV{mysqlslurp_pass}" if ( $ENV{mysqlslurp_pass} );
           $command .= " -h$ENV{mysqlslurp_host}" if ( $ENV{mysqlslurp_host} );
           
        `$command -e"drop table if exists test.mysqlslurp"` ;
        `$command -e"
            create table test.mysqlslurp 
            ( a char(25), b char(25) ) 
        "`;

        ok( $load->open, 'Method: open' );
        ok( -p $load->fifo, 'Pipe opened' );
                                                       
        ok( (print $load "a\tb\n") == 1, 'Print to FIFO successful' );

        ok( $load->close, 'Method: close' );
        ok( ! -p $load->fifo , 'FIFO successfully removed' );
        ok( ! -d $load->dir, 'Temporary directory successfully removed' );

        `$command -e"drop table if exists test.mysqlslurp"`;

    }
    
    

1;
