#!/usr/bin/perl

use strict;
use warnings;
use utf8;
use Test::More;
use Data::Dumper;

use DBI;
use Test::mysqld;

my $mysqld= Test::mysqld->new;
my $dbh= DBI->connect($mysqld->dsn);
$dbh->do("TRUNCATE performance_schema.events_errors_summary_global_by_error");

subtest "Create database" => sub
{
  ok($dbh->do("CREATE DATABASE d1 CHARSET utf8"), "Execute SQL");
  #ok(!(@{&show_warnings}), "Without warning") or diag(Dumper &show_warnings);
};
  
my $create_table= << 'EOS'
CREATE TABLE d1.t1 (
  num int(11) NOT NULL PRIMARY KEY auto_increment,
  val varchar(32) NOT NULL,
  dt DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
)
EOS
;

subtest "Create table" => sub
{
  ok($dbh->do($create_table), "Execute SQL");
  #ok(!(@{&show_warnings}), "Without warning") or diag(Dumper &show_warnings);
};

my $insert= << 'EOS'
INSERT INTO d1.t1 VALUES
  (1, 'one', NOW()),
  (2, 'two', NOW()),
  (3, 'three', NOW()),
  (4, 'four', NOW()),
  (5, 'five', NOW())
EOS
;

subtest "Insert data" => sub
{
  ok($dbh->do($insert), "Execute SQL");
  #ok(!(@{&show_warnings}), "Without warning") or diag(Dumper &show_warnings);
  ok($dbh->selectall_arrayref("SELECT num, val FROM d1.t1"), "Test fetching");
  #ok(!(@{&show_warnings}), "Without warning") or diag(Dumper &show_warnings);
};

subtest "Group by" => sub
{
  ok(my $rs= $dbh->selectall_arrayref("SELECT SQL_CALC_FOUND_ROWS num % 2 AS _num, COUNT(*) AS c " .
                                      "FROM d1.t1 GROUP BY _num", { Slice => {} }), "Group-by");
  #ok(!(@{&show_warnings}), "Without warning") or diag(Dumper &show_warnings);
  is_deeply($rs, [{ _num => 0, c => 2 }, { _num => 1, c => 3 }], "Expected order") or diag(Dumper $rs);
};

my $errors= "SELECT error_name, error_number " .
            "FROM performance_schema.events_errors_summary_global_by_error " .
            "WHERE sum_error_raised > 0";
diag($errors);
my $rs= $dbh->selectall_arrayref($errors, { Slice => {} });
if (@$rs)
{
  ok(0, "Without Errors/Warnings") or diag(Dumper $rs);
  my $statement= "SELECT query_sample_text " .
                 "FROM performance_schema.events_statements_summary_by_digest " .
                 "WHERE SUM_ERRORS + SUM_WARNINGS > 0";
  diag($statement);
  diag(Dumper $dbh->selectall_arrayref($statement, { Slice => {} }));
}
else
{
  ### No rows sum_error_raised > 0
  ok(1, "Without Errors/Warnings");
}


done_testing;

sub show_warnings
{
  return $dbh->selectall_arrayref("SHOW WARNINGS");
}


