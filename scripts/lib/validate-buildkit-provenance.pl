#!/usr/bin/env perl
use strict;
use warnings;
use JSON::PP qw(decode_json);

@ARGV == 7 or die "usage: validate-buildkit-provenance.pl FILE MODE ARCHIVE_SHA CONTRACT_SHA SOURCE_COMMIT BUILDCTL_SHA BUILDKITD_SHA\n";
my ($path, $mode, $archive_sha, $contract_sha, $source_commit, $buildctl_sha, $buildkitd_sha) = @ARGV;
open my $file, '<', $path or die "open $path: $!\n";
local $/;
my $bytes = <$file>;
length($bytes) <= 131072 or die "$path exceeds the provenance size limit\n";
my $value = eval { decode_json($bytes) };
$@ eq '' && ref($value) eq 'HASH' or die "$path is not a JSON object\n";

sub require_equal {
    my ($label, $actual, $expected) = @_;
    defined($actual) && !ref($actual) && "$actual" eq "$expected"
        or die "$path has invalid $label\n";
}

require_equal('schema_version', $value->{schema_version}, '1');
require_equal('source_mode', $value->{source_mode}, $mode);
require_equal('verdict', $value->{verdict}, 'passed');
require_equal('source_commit', $value->{source_commit}, $source_commit);
require_equal('contract_sha256', $value->{contract_sha256}, $contract_sha);
ref($value->{archive}) eq 'HASH' or die "$path lacks archive provenance\n";
require_equal('archive.sha256', $value->{archive}->{sha256}, $archive_sha);
ref($value->{entries}) eq 'ARRAY' && @{$value->{entries}} == 3
    or die "$path has invalid entry provenance\n";
require_equal('buildctl sha256', $value->{entries}->[1]->{sha256}, $buildctl_sha);
require_equal('buildkitd sha256', $value->{entries}->[2]->{sha256}, $buildkitd_sha);
ref($value->{fallbacks}) eq 'ARRAY' && @{$value->{fallbacks}} == 0
    or die "$path records a fallback\n";
ref($value->{retries}) eq 'ARRAY' && @{$value->{retries}} == 0
    or die "$path records a retry\n";
