# Git Regional Languages

More famous languages may be written everywhere 
in the world, yet the languages with lesser users 
may probably tend to be epidemic or popular 
in some particular regions. Let's find out.

---

## Getting started

Prerequisites:

- [x] Github API access token
- [x] GoogleMap API key
- [x] MongoDB
- [x] Python 3.4+
- [x] Node.js 4.x+


Since it uses [github API](https://developer.github.com/v3), 
you'll need to supply your access token. 
Create a text file named `API-TOKEN` in the root directory 
of this repository and put your Github login ID 
and your access token in the following line like so:

```
johndoeagain
f00ba478293cbbae0a6552
```

Also, [GoogleMap API key](https://developers.google.com/maps/documentation/geocoding/get-api-key) is also required for geocoding 
API access. You'll need to generate your own API key 
can place it in a text file named `GOOGLE-API-KEY`, 
put it in the root directory of the repo.


### Prepare your MongoDB

To set your data storage, create a DB named `gitlang`. 
Don't need to create any initial collection because 
the script will do it for you.

>**CAVEAT** It really requires a huge capacity to 
store the data and you can make [up to 5000 requests](https://developer.github.com/v3/#rate-limiting) 
in a span of one hour.

---