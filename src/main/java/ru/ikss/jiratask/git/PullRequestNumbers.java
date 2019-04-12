package ru.ikss.jiratask.git;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;

public class PullRequestNumbers {

    @JsonAlias("last_number")
    private Integer lastNumber;

    @JsonAlias("open_numbers")
    private List<Integer> openNumbers;

    public Integer getLastNumber() {
        return lastNumber;
    }

    public List<Integer> getOpenNumbers() {
        return openNumbers;
    }

    @Override
    public String toString() {
        return "PullRequestNumbers [lastNumber=" + lastNumber + ", openNumbers=" + openNumbers + "]";
    }

    public static void main(String[] args) throws IOException {
        PullRequestNumbers v = JsonMapper.getInstance(false).readValue("{\"last_number\" : 3, \"open_numbers\" : [1, 3]}", PullRequestNumbers.class);
        System.out.println(v);
    }
}
