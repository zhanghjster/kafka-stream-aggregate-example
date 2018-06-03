package stream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.TreeMap;

public class Json {

    public static void main(String args[]) throws IOException {
        ObjectMapper mapper = new ObjectMapper();


        //  设置输出tab
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        // 设置map的key排序
       // mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

        // 不包含为空的值
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        User user = new User("Ben", 10);

        user.addBook("apache", "lee");
        user.addBook("kafka", "benx");
        user.addBook("1", "a");
        user.addBook("good", "yes");

        user.setFriend(new Friend("Lee", 19));

        mapper.writeValue(System.out, user);

    }

    // @JsonPropertyOrder({"age", "name", "books"}
    @JsonPropertyOrder(alphabetic=true)
    public static class User {
        private String name;
        private long age;
        private TreeMap<String, String> books;
        private Friend friend;

        public Friend getFriend() {
            return friend;
        }

        public void setFriend(Friend friend) {
            this.friend = friend;
        }


        public User (TreeMap<String, String> books) {
            this.books = books;
        }
        public User(String name, long age) {
            this.name = name;
            this.age = age;
        }

        public void setNickName(String nickName) {
            this.nickName = nickName;
        }

        public String getNickName() {
            return nickName;
        }

        private String nickName;


        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getAge() {
            return age;
        }

        public void setAge(long age) {
            this.age = age;
        }

        public TreeMap<String, String> getBooks() {
            return books;
        }

        public void setBooks(TreeMap<String, String> books) {
            this.books = books;
        }

        public void addBook(String name, String author) {
            if (this.books == null) {
                this.books = new TreeMap<>();
            }

            this.books.put(name, author);
        }
    }

    @JsonPropertyOrder(alphabetic=true)
    public static class Friend {
        public Friend(String name, long age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getAge() {
            return age;
        }

        public void setAge(long age) {
            this.age = age;
        }

        private String name;
        private long age;
    }
}


