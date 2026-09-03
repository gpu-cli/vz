#!/usr/bin/env perl
use strict;
use warnings;

@ARGV == 1 or die "usage: validate-static-arm64-elf.pl BINARY\n";
my $path = $ARGV[0];
open my $file, '<:raw', $path or die "open $path: $!\n";
my $file_size = -s $file;
read($file, my $header, 64) == 64 or die "$path has a truncated ELF header\n";
substr($header, 0, 4) eq "\x7fELF" or die "$path is not ELF\n";
ord(substr($header, 4, 1)) == 2 or die "$path is not ELF64\n";
ord(substr($header, 5, 1)) == 1 or die "$path is not little-endian ELF\n";
ord(substr($header, 6, 1)) == 1 or die "$path has an invalid ELF ident version\n";
ord(substr($header, 7, 1)) == 0 or die "$path does not use the System V ABI\n";
unpack('v', substr($header, 16, 2)) == 2 or die "$path is not ET_EXEC\n";
unpack('v', substr($header, 18, 2)) == 183 or die "$path is not AArch64\n";
unpack('V', substr($header, 20, 4)) == 1 or die "$path has an invalid ELF version\n";
my $program_offset = unpack('Q<', substr($header, 32, 8));
my $program_entry_size = unpack('v', substr($header, 54, 2));
my $program_count = unpack('v', substr($header, 56, 2));
$program_offset >= 64 or die "$path has an invalid program header offset\n";
$program_entry_size == 56 or die "$path has invalid program headers\n";
$program_count > 0 && $program_count <= 128 or die "$path has an invalid program header count\n";
$program_offset + ($program_entry_size * $program_count) <= $file_size
    or die "$path program headers exceed the file size\n";
for my $index (0 .. $program_count - 1) {
    seek($file, $program_offset + ($index * $program_entry_size), 0) or die "seek $path: $!\n";
    read($file, my $program_header, $program_entry_size) == $program_entry_size
        or die "$path has a truncated program header\n";
    my $type = unpack('V', substr($program_header, 0, 4));
    $type != 3 or die "$path contains PT_INTERP\n";
    $type != 2 or die "$path contains PT_DYNAMIC\n";
}
