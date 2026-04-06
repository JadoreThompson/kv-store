package com.zenz.kvstore.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Trie {

    private final char character;
    private boolean isWord;
    private Map<Character, Trie> children;

    public Trie(char character) {
        this.character = character;
    }

    public Trie(char character, boolean isWord) {
        this.character = character;
        this.isWord = isWord;
    }

    public void add(String word) {
        add(word, 0);
    }

    private void add(final String word, final int index) {
        if (index >= word.length()) {
            return;
        }

        if (this.children == null) {
            this.children = new HashMap<Character, Trie>();
        }

        final char character = word.charAt(index);

        if (this.children.containsKey(character)) {
            final Trie trie = this.children.get(character);
            trie.add(word, index + 1);
            if (index + 1 == word.length()) {
                trie.isWord = true;
            }
        } else {
            final Trie trie = new Trie(character, index + 1 == word.length());
            this.children.put(character, trie);
            trie.add(word, index + 1);
        }
    }

    public boolean remove(String word) {
        return remove(word, 0);
    }

    private boolean remove(final String word, final int index) {
        if (index >= word.length()) {
            return false;
        }

        final char character = word.charAt(index);

        if (this.children == null || !this.children.containsKey(character)) {
            return false;
        }

        final Trie trie = this.children.get(character);
        if (index + 1 == word.length()) {
            if (!trie.isWord) {
                return false;
            }

            trie.isWord = false;
            if (trie.children == null || trie.children.isEmpty()) {
                this.children.remove(character);
            }
            return true;
        }

        final boolean result = trie.remove(word, index + 1);
        if (result && !trie.isWord && (trie.children == null || trie.children.isEmpty())) {
            this.children.remove(character);
        }

        return result;
    }

    public List<String> search(String prefix) {
        Trie trie = this;
        boolean found = true;
        for (int i = 0; i < prefix.length(); i++) {
            final char character = prefix.charAt(i);
            if (trie.children != null && trie.children.containsKey(character)) {
                trie = trie.children.get(character);
            } else {
                found = false;
                break;
            }
        }

        if (!found) {
            return null;
        }

        return extractWords(trie, new ArrayList<String>(), prefix);
    }

    private List<String> extractWords(final Trie trie, final List<String> words, final String prefix) {
        if (trie.isWord) {
            words.add(prefix);
        }

        if (trie.children != null) {
            for (final Map.Entry<Character, Trie> entry : trie.children.entrySet()) {
                extractWords(entry.getValue(), words, prefix + entry.getKey());
            }
        }

        return words;
    }

    public char getCharacter() {
        return character;
    }

    public boolean isWord() {
        return isWord;
    }
}
