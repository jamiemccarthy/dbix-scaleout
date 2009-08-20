use warnings;
use strict;
use Module::Build;
use File::Temp qw( tempfile );

=head1

see Module::Build::Cookbook I hope for tips here
(like around "Adding new file types to the build process" and esp. "Modifying an action")

idea is to subclass, override ACTION_install, construct Dumper output
manually in perl from the notes, and print it to the dbinst .pm files
and the setup .txt file.

Still haven't decided whether the .pm files that will be require'd to
gain db access should be DBIx::ScaleOut::Setup::foo.pm or
DBIx::ScaleOut::Access::foo.pm.  The Setup module itself should
be called Setup though.

build parameter 'script_files' will install scripts like
	dxso_setup	re-edit the setup access parameters

=cut

##############################

my $class = Module::Build->subclass(
	class => 'Module::Build::DXSO',
	code => <<'EOSUBCLASS' );

sub ACTION_install {
	my($self) = @_;
	my $configtext = $self->notes('configtext');
	my $dbinst_tuples = $self->notes('dbinst_tuples');
	# 1. write configtext to __DATA__ segment (?) of DBIx/ScaleOut/Setup.pm
	#    and chown as the current user, chmod 600.
	# 2. write Dumper()s of tuples to DBIx/ScaleOut/Access/foo.pm
	#    and chown/chmod them appropriately, as possible
	$self->SUPER::ACTION_install;
}

EOSUBCLASS

##############################

my $build = Module::Build->new(
	module_name	=> 'DBIx::ScaleOut',
	license		=> 'perl', # dual-license, either Artistic or GPL, your choice
	requires	=> {
		perl		=> '5.6.1',
		DBI		=> '1.38', # for last_insert_id
		'DBD::mysql'	=> '3.0', # no real reason to require 3, 2 would probably work
		'Data::Dumper'	=> '2.0',
		'Net::Ping'	=> '2.0',
		'File::Spec'	=> '3.0',
	},
	create_license	=> 1,
	create_readme	=> 1,
	dynamic_config	=> 1,
#	dist_abstract	=> 'a smart database access layer',
#	dist_author	=> 'Jamie McCarthy <jamie@mccarthy.vg>',
	# see Module::Build::API auto_features()
);

get_params();

$build->create_build_script;

############################################################

sub get_params {
	require "lib/DBIx/ScaleOut/Setup.pm";
	my $setup = DBIx::ScaleOut::Setup->new();

	sub _prompt_callback {
		my($setup, $prompt, $default) = @_;
		return $build->prompt($prompt, $default);
	}
	my($configtext, $dbinsts_hr) = $setup->do_edit(\&_prompt_callback);

	# notes() makes the values available everywhere else in the build
	# process, see e.g. Module::Build::API on current().
	my $tuples_ar = convert_dbinsts_to_tuples($dbinsts_hr);
	$build->notes(configtext => $configtext);
	$build->notes(dbinst_tuples => @$tuples_ar);
}

sub convert_dbinsts_to_tuples {
	my($dbinsts_hr) = @_;
use Data::Dumper;
print STDERR "convert_dbinsts_to_tuples ar: " . Dumper($dbinsts_hr);
	my @tuples = ( );
	for my $dbinst (sort keys %$dbinsts_hr) {
		my $hr = $dbinsts_hr->{$dbinst};
		push @tuples, { "${dbinst}_dbinst", $dbinst };
		for my $key (sort keys %$hr) {
			next if $key eq 'dbinst'; # did this one already
			push @tuples, { "${dbinst}_$key", $hr->{$key} };
		}
	}
	return \@tuples;
}

# XXX remember to strip off unnecessary semicolon from end of sql statements

