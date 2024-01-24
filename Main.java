package model;

import model.Datasource;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        Datasource datasource = new Datasource();
        if(!datasource.open()) {
            System.out.println("Can't open datasource");
            return;
        }

//        List<Artist> artists = datasource.queryArtists(Datasource.ORDER_BY_ASC);
//        if(artists == null){
//            System.out.println("No artists");
//            return;
//        }
//
//        for(Artist artist: artists){
//            System.out.println("ID " + artist.getId() + ", Name = " + artist.getName());
//        }
//          advantages and disadvantages of obtaining data via column name vs via column index. Via index is faster?
//          but by name, if you add another column or something that impacts the index, the code is not impacted


//        List<String> albumsForArtist = datasource.queryAlbumsForArtist("Iron Maiden", Datasource.ORDER_BY_ASC);
//        for(String album: albumsForArtist){
//            System.out.println(album);
//        }


        List<SongArtist> songArtists = datasource.queryArtistsForSong("Heartless", Datasource.ORDER_BY_ASC);
//        // note the quotes "Heartless". the \" negates the "" as part of this passed content
//        if(songArtists == null){
//            System.out.println("Couldn't find the artist for the song");
//            return;
//        }
//
//        for(SongArtist artist : songArtists){
//            System.out.println("Artist name = " + artist.getArtistName() + ". Album name  = " + artist.getAlbumName() +
//                    ". Track = " + artist.getTrack());
//        }
//
//        datasource.querySongsMetadata();

//        int count = datasource.getCount(Datasource.TABLE_SONGS); // there is a lot of separation going on here to make it
//        // so we don't really need to know we are connecting to a database here from a code perspective... datasource object
//        // created and connection established. called getCount method to return int. Passing field from Datasource class
//        // who's String value is 'songs'. This returns the number of songs in the songs table
//        System.out.println("Number of songs is: " + count);

//        datasource.createViewForSongArtists();

        songArtists = datasource.querySongInfoView("Go Your Own Way");
        if(songArtists.isEmpty()) { // checking for empty actually better than checking for null
            System.out.println("Couldn't find the artist for the song");
            return;
        }

        for(SongArtist artist : songArtists) {
            System.out.println("FROM VIEW - Artist name = " + artist.getArtistName() +
                    " Album name = " + artist.getAlbumName() +
                    " Track number = " + artist.getTrack());
        }

        datasource.close();
    }
}


/*
- copied the music DB into the root level of project
- added JDBC driver to libraries in project structure


- don't want classes to return a result set since don't want classes that use methods in the model package to have to understnad implementation details of the model..
 */