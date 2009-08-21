use warnings;
use strict;
use Module::Build;
use File::Path;
use File::Spec;
use File::Temp qw( tempfile );
use IO::File;
use Data::Dumper;

=head1

see Module::Build::Cookbook I hope for tips here
(like around "Adding new file types to the build process" and esp. "Modifying an action")

idea is to subclass, override ACTION_install, construct Dumper output
manually in perl from the notes, and print it to the dbinst .pm files
and the setup .txt file.

build parameter 'script_files' will install scripts like
	dxso_setup	re-edit the setup access parameters

=cut

############################################################

# If we want 'perl -MDBIx::ScaleOut::Setup -e edit' to work the most
# natural way possible, then Module::Build::DXSO should probably be
# a real class (maybe rename DBIx::ScaleOut::ModuleBuild) and should
# be installed with the rest of dxso.  I believe the 'code =>' here
# will have to be passed the text of that .pm file.  However,
# DBIx::ScaleOut::Setup::edit can 'require' that module and create
# a new $build of that class.

my $class = Module::Build->subclass(
	class => 'Module::Build::DXSO',
	code => <<'EOSUBCLASS' );

use Data::Dumper;
use DBIx::ScaleOut::Setup;

sub ACTION_install {
	my($self) = @_;
#$Data::Dumper::Sortkeys = 1;
#print STDERR "self: " . Dumper($self);
	my $configtext = $self->notes('configtext');
	my $dbinsts = $self->notes('dbinsts');
	$Data::Dumper::Sortkeys = 1;

	my $installdirs = $self->{properties}{installdirs};
	my $install_lib = $self->{properties}{install_sets}{$installdirs}{lib};

	# XXX if non-root user wants to install, here's where they
	# write the files.  Pretty sure I have $install_lib correct.
	# Probably want to chown (and if it fails, of course, abort).
	# If we decide to forbid installation except by root, here's
	# the place to check euid.
	my $setupfile_dir = File::Spec->catdir($install_lib,
		'DBIx', 'ScaleOut', 'Setup');
	File::Path::make_path($setupfile_dir, { mode => 0755 });
	my $setupfilename = File::Spec->catfile($setupfile_dir,
		DBIx::ScaleOut::Setup::SETUPFILENAME);
	my $old_umask = umask 0077;
	if (! open(my $fh, '>', $setupfilename)) {
		die "cannot write $setupfilename: $!";
	} else {
		my $text = get_setupfile_text( $self->notes('configtext') );
		print $fh $text;
		close $fh;
		print "Installing $setupfilename\n";
	}

	my $access_dir = File::Spec->catdir($install_lib,
		'DBIx', 'ScaleOut', 'Access');
	File::Path::make_path($access_dir, { mode => 0755 });
	my $dbinst_hr = $self->notes('dbinsts');
	umask 0007;
	for my $dbinst (sort keys %$dbinst_hr) {
		my $dbinstfilename = File::Spec->catfile($access_dir, "$dbinst.pm");
		my $unixuser = $dbinst_hr->{$dbinst}{unixuser};
		my $uid = getpwnam($unixuser);
		die "cannot find unix user '$unixuser' for dbinst '$dbinst'"
			if !defined($uid) || !length($uid);
		my $unixgroup = $dbinst_hr->{$dbinst}{unixgroup};
		my $gid = getgrnam($unixgroup);
		die "cannot find unix group '$unixgroup' for dbinst '$dbinst'"
			if !defined($gid) || !length($gid);
		if (! open(my $fh, '>', $dbinstfilename)) {
			die "cannot write $dbinstfilename: $!";
		} else {
			my $text = get_dbinst_text($dbinst, $dbinst_hr->{$dbinst});
			print $fh $text;
			close $fh;
			my $count = chown $uid, $gid, $dbinstfilename;
			if (!$count) {
				# XXX does chown return 0 if no change was necessary? if so this isn't an error
				my $chown_err = $!; # XXX look this up
				unlink $dbinstfilename;
				die "could not chown($uid, $gid, $dbinstfilename): $chown_err";
			}
			print "Installing $dbinstfilename\n";
		}

	}

	umask $old_umask;

	$self->SUPER::ACTION_install;
}

sub get_setupfile_text {
	my($text) = @_;
	my $dumped = Dumper($text);
	$dumped =~ s/^\$VAR1 /\$DBIx::ScaleOut::Setup::setupfile /;
	return qq{$dumped\n1;};
}

sub get_dbinst_text {
	my($dbinst, $dbinst_hr) = @_;
	my $dumped = Dumper($dbinst_hr);
	$dumped =~ s/^\$VAR1 /\$DBIx::ScaleOut::Access::$dbinst /;
	# a text comment noting creation timestamp+user, and a "do not edit
	# manually" suggestion, would be nice
	return qq{$dumped\n1;};
}

EOSUBCLASS

############################################################

my $build = $class->new(
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

$build->create_build_script;

set_notes_permissions();

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
	$build->notes(configtext => $configtext);
	$build->notes(dbinsts => $dbinsts_hr);
}

sub set_notes_permissions {
	my $notes = File::Spec->catfile($build->base_dir, '_build', 'notes');
	chmod 0600, $notes;
}

