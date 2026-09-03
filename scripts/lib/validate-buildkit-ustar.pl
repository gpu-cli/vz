#!/usr/bin/env perl
use strict;
use warnings;

@ARGV == 2 or die "usage: validate-buildkit-ustar.pl ARCHIVE INVENTORY\n";
my ($archive_path, $inventory_path) = @ARGV;
open my $archive, '<:raw', $archive_path or die "open $archive_path: $!\n";
open my $inventory, '>', $inventory_path or die "open $inventory_path: $!\n";

my @expected = (
    ['manifest.json', 0644],
    ['bin/buildctl',   0755],
    ['bin/buildkitd',  0755],
);

sub field_string {
    my ($header, $offset, $length) = @_;
    my $value = substr($header, $offset, $length);
    $value =~ s/\0.*\z//s;
    return $value;
}

sub octal_field {
    my ($header, $offset, $length, $label) = @_;
    my $raw = substr($header, $offset, $length);
    $raw =~ s/[\0 ]+\z//;
    $raw =~ s/^ +//;
    $raw =~ /\A[0-7]+\z/ or die "$label is not an octal ustar field\n";
    return oct($raw);
}

sub all_zero {
    return $_[0] !~ /[^\0]/;
}

for my $index (0 .. $#expected) {
    read($archive, my $header, 512) == 512 or die "truncated header for entry $index\n";
    !all_zero($header) or die "archive ended before required entry $index\n";

    my ($expected_name, $expected_mode) = @{$expected[$index]};
    my $name = field_string($header, 0, 100);
    $name eq $expected_name or die "entry $index is $name, expected $expected_name\n";
    substr($header, 257, 6) eq "ustar\0" or die "$name is not POSIX ustar\n";
    substr($header, 263, 2) eq '00' or die "$name has invalid ustar version\n";
    field_string($header, 345, 155) eq '' or die "$name uses a ustar prefix\n";
    field_string($header, 157, 1) =~ /\A(?:|0)\z/ or die "$name is not a regular file\n";
    octal_field($header, 100, 8, "$name mode") == $expected_mode or die "$name has wrong mode\n";
    octal_field($header, 108, 8, "$name uid") == 0 or die "$name has nonzero uid\n";
    octal_field($header, 116, 8, "$name gid") == 0 or die "$name has nonzero gid\n";
    octal_field($header, 136, 12, "$name mtime") == 0 or die "$name has nonzero mtime\n";
    field_string($header, 265, 32) eq 'root' or die "$name uname is not root\n";
    field_string($header, 297, 32) eq 'root' or die "$name gname is not root\n";
    octal_field($header, 329, 8, "$name device major") == 0 or die "$name has nonzero device major\n";
    octal_field($header, 337, 8, "$name device minor") == 0 or die "$name has nonzero device minor\n";
    all_zero(substr($header, 500, 12)) or die "$name has nonzero reserved header bytes\n";

    my $stored_checksum = octal_field($header, 148, 8, "$name checksum");
    my $checksum_header = $header;
    substr($checksum_header, 148, 8, '        ');
    my $computed_checksum = 0;
    $computed_checksum += $_ for unpack('C*', $checksum_header);
    $stored_checksum == $computed_checksum or die "$name header checksum is invalid\n";

    my $size = octal_field($header, 124, 12, "$name size");
    print {$inventory} "$name\n" or die "write $inventory_path: $!\n";
    my $payload_blocks = int(($size + 511) / 512);
    my $payload_bytes = $payload_blocks * 512;
    read($archive, my $payload, $payload_bytes) == $payload_bytes or die "$name payload is truncated\n";
    my $padding = substr($payload, $size);
    all_zero($padding) or die "$name has nonzero payload padding\n";
}

read($archive, my $tail, 512) == 512 or die "archive lacks first end block\n";
all_zero($tail) or die "archive contains an unexpected fourth entry\n";
read($archive, $tail, 512) == 512 or die "archive lacks second end block\n";
all_zero($tail) or die "archive second end block is nonzero\n";
while (read($archive, $tail, 512)) {
    length($tail) == 512 or die "archive has a partial trailing block\n";
    all_zero($tail) or die "archive has nonzero data after its end blocks\n";
}
close $inventory or die "close $inventory_path: $!\n";
