#!/usr/bin/perl

use strict;

my $file = "";
my $first = 1;
my $emit = 0;

while(<>) {
    if ($first) {
        $_ =~ m/^#\s*\d+\s+"(.*)"/;
        $file = $1;
        $first = 0;
    } elsif ($_ =~ m/^#\s*\d+\s+"(.*)"/) {
        $emit = ($1 eq $file);
    } elsif ($emit) {
        print $_;
    }
}
