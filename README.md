# Capparr
A Python3 application for monitoring and saving live streams from various camsites.

Inspired by [Recordurbate](https://github.com/oliverjrose99/Recordurbate)

## Supported sites
| Site name     | Abbreviation | Aliases                     | Quirks                 | Selectable resolution |
|---------------|--------------|-----------------------------|------------------------|-----------------------|
| Bongacams     | `BC`         |                             |                        | Yes                   |
| Cam4          | `C4`         |                             |                        | Yes                   |
| Cams.com      | `CC`         |                             |                        | Currently only 360p   |
| CamSoda       | `CS`         |                             |                        | Yes                   |
| Chaturbate    | `CB`         |                             |                        | Yes                   |
| DreamCam      | `DC`         |                             |                        | No                    |
| DreamCam VR   | `DCVR`       |                             | for VR videos          | No                    |
| FanslyLive    | `FL`         |                             |                        | Yes                   |
| Flirt4Free    | `F4F`        |                             |                        | Yes                   |
| MyFreeCams    | `MFC`        |                             |                        | Yes                   |
| SexChat.hu    | `SCHU`       |                             | use the id as username | No                    |
| StreaMate     | `SM`         | PornHubLive, PepperCams,... |                        | Yes                   |
| StripChat     | `SC`         | XHamsterLive,...            | must add crypto keys   | Yes                   |
| StripChat VR  | `SCVR`       |                             | for VR videos          | No                    |
| XLoveCam      | `XLC`        |                             |                        | No                    |


There are hundreds of clones of the sites above, you can read about them on [this site](https://adultwebcam.site/clone-sites-by-platform/).

## Requirements
* Python 3
  * Install packages listed in requirements.txt with pip.
* FFmpeg

## Usage

The application has the following interfaces:
* Console
* External console via ZeroMQ (sort of working)
* Web interface

#### Starting and console
Start the downloader (it does not fork yet)\
Automatically imports all streamers from the config file.
```
python3 Downloader.py
```

On the console you can use the following commands:
```
add <username> <site> - Add streamer to the list (also starts monitoring)
remove <username> [<site>] - Remove streamer from the list
start <username> [<site>] - Start monitoring streamer
start * - Start all
stop <username> [<site>] - Stop monitoring
stop * - stop all
status - Status display 
status2 - A slightly more readable status table
quit - Clean exit (Pressing CTRL-C also behaves like this)
```
For the `username` input, you usually have to enter the username as represented in the original URL of the room. 
Some sites are case-sensitive.

For the `site` input, you can use either the full or the short format of the site name. (And it is case-insensitive)

#### "Remote" controller
Add or remove a streamer to record (Also saves config file)
```
python3 Controller.py add <username> <website>
python3 Controller.py remove <username>
```

Start/stop recording streamers
```
python3 Controller.py <start|stop> <username>
```

List the streamers in the config
```
python3 Controller.py status
```

#### Web interface

You can access the web interface on port 5000. 
If set password in parameters.py username is admin, password admin, empty password is also allowed.
When you set the WEBSERVER_HOST it is also accesible to from other computers in the network

## Docker support

The repository includes an example `docker-compose.yml` that pulls a prebuilt
GitHub Container Registry image by default:

The repository includes an example `docker-compose.yml` that pulls
`maferick/smonitor:master` by default (override with `STREAMONITOR_IMAGE` in
`stack.env` if needed). To start it locally:

```
docker compose up -d
```

If you prefer using a prebuilt image, add an `image:` entry to
`docker-compose.yml` that points at your registry tag.

### Portainer Git Stack deploy

Use the repository `docker-compose.yml` directly in a Git Stack and configure
environment variables as `KEY=VALUE` entries only.

Important: do **not** paste bind mounts into Portainer env vars (for example,
`- /host/path:/container/path`). That syntax belongs in `volumes:` and causes
`unexpected character '/' in variable name` when used in `stack.env`.

A ready-to-copy template is provided in [`stack.env.example`](stack.env.example).
For Synology-style paths, set:

* `DOWNLOADS_PATH=/volume1/Media/Media/media/cam`
* `CONFIG_PATH=/volume2/Docker/streamonitor/config.json`
* `PARAMETERS_PATH=/volume2/Docker/streamonitor/parameters.py`
* `WEB_PORT=5000`

Then deploy the stack.

### Optional: publish from your own fork

If you want to publish a prebuilt image from your own fork, the CI can push to
Docker Hub when you configure two repository secrets under
**Settings → Secrets and variables → Actions**:

* `DOCKERHUB_USERNAME` — your Docker Hub username (e.g. `maferick`)
* `DOCKERHUB_TOKEN` — a Docker Hub access token with **Read & Write**
  permissions (create one at https://hub.docker.com/settings/security)

The image is pushed as `<DOCKERHUB_USERNAME>/smonitor:<branch-name>`, so
pushes to `master` land at `<DOCKERHUB_USERNAME>/smonitor:master`.

## Configuration

You can set some parameters in the [parameters.py](parameters.py).

You also have to add decryption keys yourself for StripChat in the `stripchat_mouflon_keys.json` file.

## Disclaimer

This program is only a proof of concept and education project, I don't encourage anybody to use it. \
Most (if not every) streamers disallow recording their shows. Please respect their wish. \
If you don't, and you record them despite this request, please don't ever publish or share any recordings. \
If you either record or share the recorded shows, you might be legally punished. \
Also, please don't use this tool for monetization in any way.
