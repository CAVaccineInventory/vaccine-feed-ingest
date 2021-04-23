#! /usr/bin/env bash


maybe_install() {

	if [ "xx$(which $1)" == "xx" ]; then
		echo "$2 doesn't seem to be installed locally, but I can do it for you."
		echo ""
		echo "Hit Control-C if you don't want me to install $2 for you"
		read 
		$3
	else 
		echo "Found your $2 install"
	fi
}



setup_macos() {

	echo "I think you're running macOS. So we'll use homebrew";
maybe_install "brew" "Homebrew" '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"';

	maybe_install "python3.9" "Python 3.9"  "brew install python@3.9"
	maybe_install "poetry" "Poetry"  "brew install poetry"
	maybe_install "gdal-config" "GDAL" "brew install gdal"
	if [ "$(gdal-config --version)" != "3.2.2" ]; then
		maybe_install "gdal-3.2.2" "a new enough GDAL" "brew upgrade gdal"

	fi
	echo "Installing all of our python dependencies using Poetry."
	echo "(It's ok to run this more than once.)"
	
	poetry install --extras lint

	echo "Install done."
	echo ""
	echo "Try this command next:"
	echo ""
	echo "poetry run vaccine-feed-ingest --help"
}

setup_linux() {
	echo "I think you're running Linux"
	if [ "xx$(which apt-get)" == "xx" ]; then
		echo "It looks like you don't have an 'apt-get' command."
		echo ""
		echo "You're probably running an RPM-based distribution."
		echo ""
		echo "We don't yet have automated setup for RPM-based distributions"
		echo "and would be absolutely delighted to take a patch."
	fi
}


case "$OSTYPE" in
	darwin*)  setup_macos; ;;
  linux*)   setup_linux ;;
  *)        echo "Sorry. We're not set up to set up $OSTYPE" ;;
esac

