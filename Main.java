package model;

import model.Datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        // ------  OPENS DB CONNECTION -----
        Datasource datasource = new Datasource();
        if(!datasource.open()) {
            System.out.println("Can't open datasource");
            return;
        }


        // ------  QUERY ARTISTS -----
//        List<Artist> artists = datasource.queryArtists(Datasource.ORDER_BY_ASC);
//        if(artists == null){
//            System.out.println("No artists");
//            return;
//        }
//        for(Artist artist: artists){
//            System.out.println("ID " + artist.getId() + ", Name = " + artist.getName());
//        }
//          advantages and disadvantages of obtaining data via column name vs via column index. Via index is faster?
//          but by name, if you add another column or something that impacts the index, the code is not impacted


        // ------  QUERY ALBUMS FOR ARTIST -----
//        List<String> albumsForArtist = datasource.queryAlbumsForArtist("Iron Maiden", Datasource.ORDER_BY_ASC);
//        for(String album: albumsForArtist){
//            System.out.println(album);
//        }


        // ------  QUERY ARTISTS FOR SONG -----
//        List<SongArtist> songArtists = datasource.queryArtistsForSong("Heartless", Datasource.ORDER_BY_ASC);
//        // note the quotes "Heartless". the \" negates the "" as part of this passed content
//        if(songArtists == null){
//            System.out.println("Couldn't find the artist for the song");
//            return;
//        }
//        for(SongArtist artist : songArtists){
//            System.out.println("Artist name = " + artist.getArtistName() + ". Album name  = " + artist.getAlbumName() +
//                    ". Track = " + artist.getTrack());
//        }


        // ------  QUERY SONGS META DATA -----
//        datasource.querySongsMetadata();


        // ------  GET ROW COUNT OF SPECIFIED TABLE -----
//        int count = datasource.getCount(Datasource.TABLE_SONGS); // there is a lot of separation going on here to make it
//        // so we don't really need to know we are connecting to a database here from a code perspective... datasource object
//        // created and connection established. called getCount method to return int. Passing field from Datasource class
//        // who's String value is 'songs'. This returns the number of songs in the songs table
//        System.out.println("Number of songs is: " + count);


        // ------  CREATE A VIEW OF: artist name, album name, album track #, song name -----
//        datasource.createViewForSongArtists();


        // ------  QUERY view and returns artist name, album and track # associated with entered song from Scanner object -----
//        Scanner scanner = new Scanner(System.in);
//        System.out.println("Enter a song title");
//        String title = scanner.nextLine();
//
//        List<SongArtist> songArtists;
//        songArtists = datasource.querySongInfoView(title);
//        if(songArtists.isEmpty()) { // checking for empty actually better than checking for null
//            System.out.println("Couldn't find the artist for the song");
//            return;
//        }
//        for(SongArtist artist : songArtists) {
//            System.out.println("FROM VIEW - Artist name = " + artist.getArtistName() +
//                    " Album name = " + artist.getAlbumName() +
//                    " Track number = " + artist.getTrack());
//        }


        datasource.insertSong("Touch of Grey", "Grateful Dead", "In The Dark", 9);

        // ------  CLOSES THE DB CONNECTION -----
        datasource.close();
    }
}


/*
- copied the music DB into the root level of project
- added JDBC driver to libraries in project structure


- don't want classes to return a result set since don't want classes that use methods in the model package to have to understnad implementation details of the model..
 */