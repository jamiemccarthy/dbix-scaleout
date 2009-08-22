use warnings;
use strict;

use lib 'lib';
use DBIx::ScaleOut::ModuleBuild;

use File::Spec;
#use File::Temp qw( tempfile );
#use IO::File;
#use Data::Dumper;

=head1

see Module::Build::Cookbook I hope for tips here
(like around "Adding new file types to the build process")

See Module::Build::Authoring, "SUBCLASSING".  DBIx::ScaleOut::ModuleBuild
overrides ACTION_install to write the dbinst .pm files and the setup
text file.

At some point, might want to write a script:
	bin/dxso_setup	re-edit the setup access parameters
which I believe will be automatically installed as one might expect, simply
by putting it in bin/, but check the build parameter 'script_files'.

=cut

############################################################

my $build = DBIx::ScaleOut::ModuleBuild->new(
	module_name	=> 'DBIx::ScaleOut',
	license		=> 'perl', # dual-license, either Artistic or GPL, your choice
	requires	=> {
		perl		=> '5.6.1',
		DBI		=> '1.38', # for last_insert_id
		'DBD::mysql'	=> '3.0', # no real reason to require 3, 2 would probably work
			# see Module::Build::API "auto_features" for a better way to demand drizzle/mysql/pg
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

# maybe 'umask 0077' around this call, to force the files in
# _build/ to be unreadable by other users?
$build->create_build_script;

set_notes_permissions();

############################################################

sub get_params {

	# This should be unnecessary, because DBIx::ScaleOut::ModuleBuild
	# has already use'd DBIx::ScaleOut::Setup.  But, I'll leave it here
	# explicitly for now.
	require DBIx::ScaleOut::Setup;

	my $setup = DBIx::ScaleOut::Setup->new();

	sub _prompt_callback {
		my($setup, $prompt, $default) = @_;
		return $build->prompt($prompt, $default);
	}
	my($configtext, $dbinsts_hr) = $setup->do_edit(\&_prompt_callback);

	# notes() makes the values available everywhere else in the build
	# process, see e.g. Module::Build::API on current().
	$build->notes(configtext => $configtext);
	$build->notes(dbinsts => $dbinsts_hr);
}

sub set_notes_permissions {
	my $notes = File::Spec->catfile($build->base_dir, '_build', 'notes');
	chmod 0600, $notes;
}

