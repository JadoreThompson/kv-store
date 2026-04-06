package com.zenz.kvstore.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test class for {@link Trie}.
 * Tests all public methods including edge cases.
 */
class TrieTest {

    private Trie root;

    @BeforeEach
    void setUp() {
        root = new Trie('\0', false);
    }

    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {

        @Test
        @DisplayName("Should create Trie with character only")
        void shouldCreateTrieWithCharacterOnly() {
            Trie trie = new Trie('a', false);
            assertEquals('a', trie.getCharacter());
            assertFalse(trie.isWord());
        }

        @Test
        @DisplayName("Should create Trie with character and isWord flag")
        void shouldCreateTrieWithCharacterAndIsWord() {
            Trie trie = new Trie('a', true);
            assertEquals('a', trie.getCharacter());
            assertTrue(trie.isWord());
        }

        @Test
        @DisplayName("Should create Trie with isWord false")
        void shouldCreateTrieWithIsWordFalse() {
            Trie trie = new Trie('b', false);
            assertEquals('b', trie.getCharacter());
            assertFalse(trie.isWord());
        }
    }

    @Nested
    @DisplayName("add() Method Tests")
    class AddMethodTests {

        @Test
        @DisplayName("Should add single word to trie")
        void shouldAddSingleWord() {
            root.add("hello");

            List<String> result = root.search("hello");
            assertNotNull(result);
            assertTrue(result.contains("hello"));
        }

        @Test
        @DisplayName("Should add multiple words to trie")
        void shouldAddMultipleWords() {
            root.add("hello");
            root.add("world");
            root.add("hi");

            List<String> helloResult = root.search("hello");
            List<String> worldResult = root.search("world");
            List<String> hiResult = root.search("hi");

            assertNotNull(helloResult);
            assertNotNull(worldResult);
            assertNotNull(hiResult);
            assertTrue(helloResult.contains("hello"));
            assertTrue(worldResult.contains("world"));
            assertTrue(hiResult.contains("hi"));
        }

        @Test
        @DisplayName("Should handle adding duplicate words")
        void shouldHandleDuplicateWords() {
            root.add("test");
            root.add("test");

            List<String> result = root.search("test");
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.contains("test"));
        }

        @Test
        @DisplayName("Should add words with common prefix")
        void shouldAddWordsWithCommonPrefix() {
            root.add("car");
            root.add("cat");
            root.add("cart");

            List<String> result = root.search("ca");
            assertNotNull(result);
            assertEquals(3, result.size());
            assertTrue(result.contains("car"));
            assertTrue(result.contains("cat"));
            assertTrue(result.contains("cart"));
        }

        @Test
        @DisplayName("Should handle empty string")
        void shouldHandleEmptyString() {
            root.add("");

            List<String> result = root.search("");
            assertNotNull(result);
        }

        @Test
        @DisplayName("Should add single character word")
        void shouldAddSingleCharacterWord() {
            root.add("a");

            List<String> result = root.search("a");
            assertNotNull(result);
            assertTrue(result.contains("a"));
        }

        @Test
        @DisplayName("Should add word that is prefix of another")
        void shouldAddWordThatIsPrefixOfAnother() {
            root.add("car");
            root.add("cart");

            List<String> carResult = root.search("car");
            List<String> cartResult = root.search("cart");

            assertNotNull(carResult);
            assertNotNull(cartResult);
            assertTrue(carResult.contains("car"));
            assertTrue(carResult.contains("cart"));
            assertTrue(cartResult.contains("cart"));
        }

        @Test
        @DisplayName("Should handle case sensitivity")
        void shouldHandleCaseSensitivity() {
            root.add("Hello");
            root.add("hello");
            root.add("HELLO");

            List<String> result = root.search("h");
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.contains("hello"));

            List<String> upperResult = root.search("H");
            assertNotNull(upperResult);
            assertEquals(2, upperResult.size());
            assertTrue(upperResult.contains("Hello"));
        }
    }

    @Nested
    @DisplayName("remove() Method Tests")
    class RemoveMethodTests {

        @Test
        @DisplayName("Should remove existing word")
        void shouldRemoveExistingWord() {
            root.add("hello");
            assertTrue(root.remove("hello"));

            List<String> result = root.search("hello");
            assertNull(result);
        }

        @Test
        @DisplayName("Should return false when removing non-existent word")
        void shouldReturnFalseWhenRemovingNonExistentWord() {
            assertFalse(root.remove("nonexistent"));
        }

        @Test
        @DisplayName("Should return false when removing from empty trie")
        void shouldReturnFalseWhenRemovingFromEmptyTrie() {
            assertFalse(root.remove("anything"));
        }

        @Test
        @DisplayName("Should remove word but keep shared prefix")
        void shouldRemoveWordButKeepSharedPrefix() {
            root.add("car");
            root.add("cat");

            assertTrue(root.remove("car"));

            List<String> result = root.search("ca");
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.contains("cat"));
            assertFalse(result.contains("car"));
        }

        @Test
        @DisplayName("Should remove word that is prefix of another word")
        void shouldRemoveWordThatIsPrefixOfAnother() {
            root.add("car");
            root.add("cart");

            assertTrue(root.remove("car"));

            List<String> result = root.search("car");
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.contains("cart"));
            assertFalse(result.contains("car"));
        }

        @Test
        @DisplayName("Should return false for partial word removal")
        void shouldReturnFalseForPartialWordRemoval() {
            root.add("hello");

            assertFalse(root.remove("he"));
        }

        @Test
        @DisplayName("Should return false for longer word removal")
        void shouldReturnFalseForLongerWordRemoval() {
            root.add("hi");

            assertFalse(root.remove("hello"));
        }

        @Test
        @DisplayName("Should handle removing same word twice")
        void shouldHandleRemovingSameWordTwice() {
            root.add("test");

            assertTrue(root.remove("test"));
            assertFalse(root.remove("test"));
        }

        @Test
        @DisplayName("Should remove single character word")
        void shouldRemoveSingleCharacterWord() {
            root.add("a");

            assertTrue(root.remove("a"));

            List<String> result = root.search("a");
            assertNull(result);
        }

        @Test
        @DisplayName("Should return false for empty string removal")
        void shouldReturnFalseForEmptyStringRemoval() {
            assertFalse(root.remove(""));
        }
    }

    @Nested
    @DisplayName("search() Method Tests")
    class SearchMethodTests {

        @Test
        @DisplayName("Should find words with given prefix")
        void shouldFindWordsWithGivenPrefix() {
            root.add("apple");
            root.add("app");
            root.add("application");
            root.add("banana");

            List<String> result = root.search("app");
            assertNotNull(result);
            assertEquals(3, result.size());
            assertTrue(result.contains("apple"));
            assertTrue(result.contains("app"));
            assertTrue(result.contains("application"));
            assertFalse(result.contains("banana"));
        }

        @Test
        @DisplayName("Should return null for non-existent prefix")
        void shouldReturnNullForNonExistentPrefix() {
            root.add("hello");

            List<String> result = root.search("xyz");
            assertNull(result);
        }

        @Test
        @DisplayName("Should return all words for empty prefix")
        void shouldReturnAllWordsForEmptyPrefix() {
            root.add("apple");
            root.add("banana");
            root.add("cherry");

            List<String> result = root.search("");
            assertNotNull(result);
            assertEquals(3, result.size());
            assertTrue(result.contains("apple"));
            assertTrue(result.contains("banana"));
            assertTrue(result.contains("cherry"));
        }

        @Test
        @DisplayName("Should return exact word when searching for complete word")
        void shouldReturnExactWordWhenSearchingForCompleteWord() {
            root.add("hello");
            root.add("helloWorld");

            List<String> result = root.search("hello");
            assertNotNull(result);
            assertTrue(result.contains("hello"));
            assertTrue(result.contains("helloWorld"));
        }

        @Test
        @DisplayName("Should return empty list when prefix exists but no complete words")
        void shouldReturnEmptyListWhenPrefixExistsButNoCompleteWords() {
            root.add("hello");

            root.remove("hello");

            List<String> result = root.search("h");
            assertNull(result);
        }

        @Test
        @DisplayName("Should handle search in empty trie")
        void shouldHandleSearchInEmptyTrie() {
            List<String> result = root.search("anything");
            assertNull(result);
        }

        @Test
        @DisplayName("Should handle search for single character prefix")
        void shouldHandleSearchForSingleCharacterPrefix() {
            root.add("a");
            root.add("ab");
            root.add("abc");

            List<String> result = root.search("a");
            assertNotNull(result);
            assertEquals(3, result.size());
        }

        @Test
        @DisplayName("Should return single word list for unique word")
        void shouldReturnSingleWordListForUniqueWord() {
            root.add("unique");

            List<String> result = root.search("unique");
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.contains("unique"));
        }
    }

    @Nested
    @DisplayName("getCharacter() Method Tests")
    class GetCharacterMethodTests {

        @Test
        @DisplayName("Should return correct character")
        void shouldReturnCorrectCharacter() {
            Trie trie = new Trie('x', false);
            assertEquals('x', trie.getCharacter());
        }

        @Test
        @DisplayName("Should return null character for root")
        void shouldReturnNullCharacterForRoot() {
            Trie root = new Trie('\0', false);
            assertEquals('\0', root.getCharacter());
        }

        @Test
        @DisplayName("Should return different characters for different nodes")
        void shouldReturnDifferentCharactersForDifferentNodes() {
            Trie nodeA = new Trie('a', false);
            Trie nodeB = new Trie('b', false);
            Trie nodeC = new Trie('c', false);

            assertEquals('a', nodeA.getCharacter());
            assertEquals('b', nodeB.getCharacter());
            assertEquals('c', nodeC.getCharacter());
        }
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should handle complex add, remove, and search operations")
        void shouldHandleComplexOperations() {
            // Add multiple words
            root.add("programming");
            root.add("program");
            root.add("programmer");
            root.add("programmatic");
            root.add("progress");

            // Verify all words exist
            List<String> progResult = root.search("prog");
            assertNotNull(progResult);
            assertEquals(5, progResult.size());

            assertTrue(root.remove("program"));

            // Verify removal
            List<String> afterRemove = root.search("prog");
            assertNotNull(afterRemove);
            assertEquals(4, afterRemove.size());
            assertFalse(afterRemove.contains("program"));

            root.add("program");

            // Verify it's back
            List<String> afterAdd = root.search("prog");
            assertNotNull(afterAdd);
            assertEquals(5, afterAdd.size());
            assertTrue(afterAdd.contains("program"));
        }

        @Test
        @DisplayName("Should handle removing all words")
        void shouldHandleRemovingAllWords() {
            root.add("one");
            root.add("two");
            root.add("three");

            assertTrue(root.remove("one"));
            assertTrue(root.remove("two"));
            assertTrue(root.remove("three"));

            List<String> result = root.search("");
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should maintain trie structure after various operations")
        void shouldMaintainTrieStructureAfterVariousOperations() {
            // Build a trie
            root.add("test");
            root.add("testing");
            root.add("tester");
            root.add("tea");

            // Remove middle branch
            assertTrue(root.remove("testing"));

            // Verify other words still exist
            List<String> testResult = root.search("test");
            assertNotNull(testResult);
            assertTrue(testResult.contains("test"));
            assertTrue(testResult.contains("tester"));
            assertFalse(testResult.contains("testing"));

            List<String> teaResult = root.search("tea");
            assertNotNull(teaResult);
            assertTrue(teaResult.contains("tea"));
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle special characters")
        void shouldHandleSpecialCharacters() {
            root.add("hello-world");
            root.add("hello_world");
            root.add("hello.world");
            root.add("hello123");

            List<String> result = root.search("hello");
            assertNotNull(result);
            assertEquals(4, result.size());
        }

        @Test
        @DisplayName("Should handle numbers in words")
        void shouldHandleNumbersInWords() {
            root.add("test123");
            root.add("test456");

            List<String> result = root.search("test");
            assertNotNull(result);
            assertEquals(2, result.size());
            assertTrue(result.contains("test123"));
            assertTrue(result.contains("test456"));
        }

        @Test
        @DisplayName("Should handle very long word")
        void shouldHandleVeryLongWord() {
            StringBuilder longWord = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                longWord.append("a");
            }
            String word = longWord.toString();

            root.add(word);

            List<String> result = root.search(word);
            assertNotNull(result);
            assertTrue(result.contains(word));
        }

        @Test
        @DisplayName("Should handle unicode characters")
        void shouldHandleUnicodeCharacters() {
            root.add("café");
            root.add("naïve");
            root.add("日本語");

            List<String> cafeResult = root.search("caf");
            assertNotNull(cafeResult);
            assertTrue(cafeResult.contains("café"));

            List<String> japaneseResult = root.search("日");
            assertNotNull(japaneseResult);
            assertTrue(japaneseResult.contains("日本語"));
        }

        @Test
        @DisplayName("Should handle whitespace in words")
        void shouldHandleWhitespaceInWords() {
            root.add("hello world");
            root.add("hello\ttab");
            root.add("hello\nnewline");

            List<String> result = root.search("hello");
            assertNotNull(result);
            assertEquals(3, result.size());
        }
    }
}