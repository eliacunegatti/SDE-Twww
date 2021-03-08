[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Feliacunegatti%2FSDE-Twww.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Feliacunegatti%2FSDE-Twww?ref=badge_shield)

# SDE-Twww
This is a service developed in Flask using Python which fetches, merges, and then processes data from the services we have previously developed which fetching data directly from the official Twitter's and Twitch's API. 
In it also linked to our MongoDB database where are stored all the monitored streams from the system and to the Cloud Firebase database where are stored
the record regarding the users of the web services.

## Features
- Search User on Twitch and Twitter
- Display User's Insights
- Display Streams Details
- Start Monitoring a new User
- Checking the presence of a User in the system
- Get Information (of Twitter and Twitch) about a User
- Check Personal Account with Google Authentication
- Add/Delete Favorites for an Account
- Get the list of favorites of an Account
- Get Trending Streamers

## Installation and setup
First of all clone the repo.
Be sure to have Python already installed on your laptop, if not install it.
After that check to have all the packages used for this project.
If you are missing some packages here are available all the bash commands in order to be sure you will install everything needed to run the code.

```bash
pip install -r requirements.txt
```

And then you must set up the ENV variables, you can find a sample [here](https://github.com/eliacunegatti/SDE-Twww/blob/main/requirements.txt)
After all the setup simply run the code using the command 
```bash
flask run
```
## Api Documentation
You can find the full API documentation [here](https://deltamangolytica.docs.apiary.io/#reference/0/get-friends-of-user/display-basic-tweets-data).
## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Feliacunegatti%2FSDE-Twww.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Feliacunegatti%2FSDE-Twww?ref=badge_large)