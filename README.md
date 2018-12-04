# PDL Challenge

Welcome!

In **data/**, you will find 10 compressed text files. Uncompressed, these 10 text files in aggregate contain 426,767 lines, where each line is a [JSON](https://www.json.org/)-[serialized](https://stackoverflow.com/questions/3316762/what-is-deserialize-and-serialize-in-json) object.

Each object contains some attributes about a single person (ostensibly). There are many duplicates in the data (ie one person is *often* represented by more than one of these records).

Your goal is to [entity-resolve](https://en.wikipedia.org/wiki/Record_linkage) (deduplicate) the data. 

For extra points you may want to do additional processing to the data once it has been entity-resolved. 

### Deliverable
Generally we prefer Python [3], but if you are more comfortable with C/C++/Java/Go, feel free to use one of these. Make sure to include any code that you write for any part of your process. The more of your process you document, the better. This includes outside research, gathering external datasets, etc. Of course, make sure to include your resulting entity-resolved records (in the same format that you received the raw data).


### Logistics

Feel free to fork this repo and work from there, or just take the data and use your own fresh repo. If the raw data is included in your local file structure, it may make sense to add it to your *.gitignore*.  Please send a link to your resulting repo to *andrew@peopledatalabs.com*.

### Effort
Feel free to spend as much time as you would like on this. We are looking for people who work hard *and* smart. This is a problem to which one could easily dedicate one hour or one year - don't lose yourself in the details. 
