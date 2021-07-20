const fs = require('fs')
const sqlite3 = require('sqlite3').verbose()
const csv = require('fast-csv')
const db = new sqlite3.Database('movies.db')
var stream = fs.createReadStream("MovieData.csv")


db.serialize(()=>{
  db.run('CREATE TABLE IF NOT EXISTS shows (id INTEGER PRIMARY KEY, title TEXT, description TEXT)')
  db.run('CREATE TABLE IF NOT EXISTS genres (show_id INTEGER, genre TEXT, FOREIGN KEY(show_id) REFERENCES shows(id))')
})


csv.parseStream(stream, {headers : true})
 .on('data', async(data)=>{
   db.serialize(()=>{
    db.run('INSERT INTO shows (title, description) VALUES(?, ?)', data.Title, data.Description, function(err){
      console.log('Error: '+err)

      let Genres = data.Genre.split(',')
      Genres.map(g=>db.run('INSERT INTO genres (show_id, genre) VALUES(?, ?)', this.lastID, g))      
    })
   })
 })
 .on('end', function(){
     console.log("done")
 }).on('error', (e)=>{
   console.log('some error happend')
 })
