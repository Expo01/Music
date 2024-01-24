package model;


import javax.swing.plaf.nimbus.State;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class Datasource {

    public static final String DB_NAME = "music.db";

    public static final String CONNECTION_STRING = "jdbc:sqlite:/Users/chrisdailey/Documents/CODING/Projects/Music/" + DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2; // ascending
    public static final int ORDER_BY_DESC = 3; // descending

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + " FROM "
            + TABLE_ALBUMS + " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID + " WHERE " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + " =\"";

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";
    // Advantage of not hard coding any names because it would be horrendous to change

    public static final String QUERY_ARTIST_FOR_SONG_START =
            "SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME +
                    ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + " FROM " + TABLE_SONGS + " INNER JOIN " +
                    TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." +
                    COLUMN_ALBUM_ID + " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST
                    + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID + " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE
                    + " = \"";

    public static final String QUERY_ARTIST_FOR_SONG_SORT =
            " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME
            + " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONG_TITLE +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS +
            "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " ORDER BY " +
            TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK;

    public static final String QUERY_VIEW_SONG_INFO =  "SELECT " + COLUMN_ARTIST_NAME + ", " +
            COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + ", " +
            COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = ?";

   private PreparedStatement querySongInfoView;

    private Connection conn;


    // --------------------- METHODS ----------------------------

    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            return true;
        } catch(SQLException e) {
            System.out.println("Couldn't connect to database: " + e.getMessage());
            return false;
        }
    } // open the connection

    public void close() {
        try {
            if(conn != null) {
                conn.close();
            }
        } catch(SQLException e) {
            System.out.println("Couldn't close connection: " + e.getMessage());
        }
    } // close the connection

    public List<Artist> queryArtists(int sortOrder) {
        StringBuilder sb = new StringBuilder("SELECT * FROM " + TABLE_ARTISTS);
        if(sortOrder != ORDER_BY_NONE){
            sb.append(" ORDER BY " + COLUMN_ARTIST_NAME + " COLLATE NOCASE ");
            if(sortOrder == ORDER_BY_DESC){
                sb.append("DESC");
            } else {
                sb.append("ASC");
                // SQL statement = SELECT * FROM artists ORDER BY name COLLATE NOCASE ASC

            }
        }

        System.out.println("SQL statement = " + sb.toString());

        try(Statement statement = conn.createStatement();
//            ResultSet results = statement.executeQuery("SELECT * FROM " + TABLE_ARTISTS)) {
            ResultSet results = statement.executeQuery(sb.toString())) {
            // this is a try with resources block? Objects are declared in the try parameters instead of
            // having to create objects assigned to null, then reassign them in the try block
            // removes the need for a finally clause which would otherwise be used to try and catch for both
            // closing results and statement
            // statement and result set automatically closed using the 'try with resources' syntax

            // DEF: a try with resources statement is a try statement that declares 1 or more resources that
            // will close after program is finished with it. Resources implement the java.lang.Autoclosable
            // interface. Resources that are in the try-with-resource are closed whether the try statement
            // completes normally or via exception error

            List<Artist> artists = new ArrayList<>();
            while(results.next()) {
                Artist artist = new Artist();
//                artist.setId(results.getInt(COLUMN_ARTIST_ID)); these are calls via column title
//                artist.setName(results.getString(COLUMN_ARTIST_NAME));
                artist.setId(results.getInt(INDEX_ARTIST_ID)); // these are calls via column index instead of the column title
                artist.setName(results.getString(INDEX_ARTIST_NAME)); // these also take an int instead of just string. overloaded?

                artists.add(artist);
            }

            return artists;

        } catch(SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }

        // why no finally block for closing results and statement?

    } // returns all column content for all artists in artists table

    public List<String> queryAlbumsForArtist(String artistName, int sortOrder){

        //SQL statement = SELECT albums.name FROM albums INNER JOIN artists ON albums.artist = artists._id WHERE
        // artists.name = "Iron Maiden" ORDER BY albums.name COLLATE NOCASE ASC
        // this is full SQL statement being built
            StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
            sb.append(artistName);
            sb.append("\"");

        if(sortOrder != ORDER_BY_NONE){
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if(sortOrder == ORDER_BY_DESC){
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL statement = " + sb.toString());

        try(Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(sb.toString())){ // ultimately here the stringbuilder just
            // becomes a string.

            List<String> albums = new ArrayList<>();
            while(results.next()){
                albums.add(results.getString(1)); // index corresponds to column in resultSet not index of
                // column in the table. indices start at 1 not zero here.
            }

            return albums;

        } catch(SQLException e){
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }

    } // uses artist ID from albums table
    // and links to ID in artist table. orders all albums alphabetically from the albums table that where artist name
    // from artist table is "Iron Maiden" as an example

    public List<SongArtist> queryArtistsForSong(String songName, int sortOrder) {

        StringBuilder sb = new StringBuilder(QUERY_ARTIST_FOR_SONG_START);
        sb.append(songName);
        sb.append("\""); // escape sequence with " negates the " as part of the SQL statement. there is one at the end
        // of the Query artist for song start String which handles the first " before the song name, then we append
        // another \" to handle the second " after the song name

        if(sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ARTIST_FOR_SONG_SORT);
            if(sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL Statement: " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<SongArtist> songArtists = new ArrayList<>();

            while(results.next()) { // SongArtist class allows creation of new object with fields for song name, album
                // name and artist name
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrack(results.getInt(3));
                songArtists.add(songArtist);
            }

            return songArtists; // List returned but not printed here
        } catch(SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    } // passing a known song name, the
    // will create a view of track #, album name and artist name using album ID to join songs and albums table
    // and artist ID to join albums and artist tables

    public void querySongsMetadata() {
        String sql = "SELECT * FROM " + TABLE_SONGS;

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sql)) {

            ResultSetMetaData meta = results.getMetaData(); //ResultSetMetaData is an object that can be used to get
            // information about the types and properties of the columns in a ResultSet object
            int numColumns = meta.getColumnCount();
            for(int i=1; i<= numColumns; i++) { // indices in table columns start at 1
                System.out.format("Column %d in the songs table is names %s\n", i, meta.getColumnName(i));
                // \n here is just a normal escape for a new line
                // 'souf' format allows passing of %d for 'i' and %s for 'meta.getColumnName(i)' for int and string
                // where 'i' is incremented so prints column index and name for all existing columns in songs table
            }
        } catch(SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
        }
    } // prints column titles from the songs table


    public int getCount(String table) {
        String sql = "SELECT COUNT(*) AS count FROM " + table; // 'AS' used to assign name to resulting column...
        try(Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(sql)) {

            int count = results.getInt("count");
//            int count = results.getInt(1); only one column from result set, returns and assigns that int val

            System.out.format("Count = %d\n", count);
            return count;
        } catch(SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return -1;
        }
    } // will return the number of rows in specified table

    public boolean createViewForSongArtists() {

        try(Statement statement = conn.createStatement()) {

            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;

        } catch(SQLException e) {
            System.out.println("Create View failed: " + e.getMessage());
            return false;
        }
    } // creates a view which is added to the database and will be viewable
    // in DB browser with a different icon indicating it is a view not a table. This particular view will have columns:
    // artist name, album name, album track #, song name
    // view will be queriable with SQL statement just like a table such as....
    // SELECT name, album, track FROM artist_list WHERE title = "Go Your Own Way"

    public List<SongArtist> querySongInfoViewNonOptimized(String title) {
        StringBuilder sb = new StringBuilder(QUERY_VIEW_SONG_INFO);
        sb.append(title);
        sb.append("\"");

        System.out.println(sb.toString());

        try (Statement statement = conn.createStatement(); // statement objects are compiled every time a query is processed.
             // there are more efficient ways to build SQL statements and to also make them less susceptible to hacking
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<SongArtist> songArtists = new ArrayList<>();
            while(results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrack(results.getInt(3));
                songArtists.add(songArtist);
            }

            return songArtists;

        } catch(SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    } // uses regular Statement class

    public List<SongArtist> querySongInfoView(String title) {

        try {
            querySongInfoView.setString(1, title); // PreparedStatement class is subclass of Statement class
            // so can execute the same methods
            ResultSet results = querySongInfoView.executeQuery();
            // don't ahve to worry about closing ResultSet when using PreparedStatements. When closing the PreparedStatement
            // whatever ResultSet is active will also be closed

            List<SongArtist> songArtists = new ArrayList<>();
            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrack(results.getInt(3));
                songArtists.add(songArtist);
            }

            return songArtists;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

}

