module github.com/99designs/keyring

go 1.14

require (
	github.com/danieljoos/wincred v1.0.2
	github.com/dvsekhvalnov/jose2go v0.0.0-20180829124132-7f401d37b68a
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c
	github.com/keybase/go-keychain v0.0.0-20190712205309-48d3d31d256d
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mtibben/percent v0.2.1
	github.com/stretchr/objx v0.2.0 // indirect
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4
	golang.org/x/sys v0.0.0-20190712062909-fae7ac547cb7 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
)

replace github.com/keybase/go-keychain => github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4
