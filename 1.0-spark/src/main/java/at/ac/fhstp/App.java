package at.ac.fhstp;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Running WordCount");
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        WordCount.wordCount(args[0], args[1]);
    }
}
