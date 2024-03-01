package dev.sangco.language;

import com.magicalpipelines.model.EntitySentiment;
import dev.sangco.serialization.Tweet;

import java.util.List;

public interface LanguageClient {

    public Tweet translate(Tweet tweet, String targetLanguage);

    public List<EntitySentiment> getEntitySentiment(Tweet tweet);

}
