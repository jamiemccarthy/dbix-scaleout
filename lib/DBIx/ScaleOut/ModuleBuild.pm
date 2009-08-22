package DBIx::ScaleOut::ModuleBuild;

# This code used to be in Build.PL, but in order to be able to
#	perl -MDBIx::ScaleOut::Setup -e edit
# without repeating code, it needs to be in a module that is
# installed normally.  See Module::Build::Authoring, "SUBCLASSING".
# See also Module::Build::Cookbook, "Modifying an action."

use Module::Build;
@ISA = qw(Module::Build);

use Data::Dumper;
use File::Path;
use File::Spec;

use DBIx::ScaleOut::Setup; # for the SETUPFILENAME constant

sub process_pm_files {
	my $self = shift;
	#print STDERR "process_pm_files called\n";
	# XXX we probably want to write the setupfile and dbinst .pm's
	# into blib/ at this time, to be copied to the installation
	# directory by the default ACTION_install later.  Arguments
	# against: (a) the default ACTION_install just calls
	# File::Copy::copy and then sets chmod to 444 or 555, rather
	# than preserving owner/perms, so we would have to chmod/chown
	# after SUPER::ACTION_install ran and that's less secure;
	# (b) doing two writes makes installation more confusing/difficult
	# and I still have to rewrite this code such that edit() can
	# share much of it.  so difficulty is not what I want to add
	# at this time.  Two arguments for it: (c) if './Build install
	# uninst=1' would probably correctly uninstall old copies
	# of those files; (d) './Build diff' would probably work on
	# those files.
	#
	# Might want to reread what the .packlist is for and whether I
	# want to add the .pm's to it.
	#
	# My current guess is that I want to
	# (1) write them into blib/ here in process_pm_files, setting
	# chmod 0600 but not chown;
	# (2) write an INSTALL.SKIP (probably in ACTION_build) which ExtUtils::Install::install
	# will respect, to avoid copying them as part of the default
	# Module::Build::ACTION_install;
	# (3) override ACTION_install to copy these files after the
	# default install, setting chmod/chown;
	# (4) do_edit() will want to write a notes file (or maybe a
	# while _build directory?); instead of calling ACTION_install,
	# just manually do the copy in step (3); rm the notes/_build.

	$self->SUPER::process_pm_files(@_);
}

sub ACTION_install {
	my $self = shift;

	print STDERR "A_i install_map: " . Dumper($self->install_map);

	my $configtext = $self->notes('configtext');
	my $dbinsts = $self->notes('dbinsts');
	$Data::Dumper::Sortkeys = 1;

#print STDERR "D:S:MB:A_i self: " . Dumper($self);

	my $installdirs = $self->{properties}{installdirs};
	my $install_lib = $self->{properties}{install_sets}{$installdirs}{lib};

	# XXX move this code into ::Setup::write_files

	# XXX installation of both SETUPFILENAME and the Access
	# dbinsts really needs to be done by writing a temp file
	# into the dir and mv'ing it into place.  Check to see if
	# there's a module for that.  Also, if its contents, chown
	# and chmod all match, skip and emit the same text emitted by
	# 'Build install' by default: "Skipping /file/name (unchanged)"
	# (from ExtUtils::Install::install).

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
	my $old_umask = umask 0077; # make _setupfile.pm mod'd -rw-------
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
	umask 0447; # make $dbinst.pm's mod'd -r--r-----
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
	my $dumped = Data::Dumper->Dump([$text],      ['DBIx::ScaleOut::Setup::setupfile']);
	return qq{$dumped\n1;};
}

sub get_dbinst_text {
	my($dbinst, $dbinst_hr) = @_;
	my $timestamp = scalar gmtime;
	my @comments = (
		qq{# dbinst record '$dbinst' for DBIx::ScaleOut},
		qq{#},
		qq{# Autogenerated by DBIx::ScaleOut::ModuleBuild at $timestamp.},
		# XXX add username, hostname?
		qq{# Do not edit manually unless you know what you're doing.},
		qq{# Best way to make changes: # perl -MDBIx::ScaleOut::Setup -e edit},
	);
	my $dumped = Data::Dumper->Dump([$dbinst_hr], ["DBIx::ScaleOut::Access::$dbinst"]);
	# a text comment noting creation timestamp+user, and a "do not edit
	# manually" suggestion, would be nice
	return join("\n", @comments, $dumped, '1;');
}

1;

