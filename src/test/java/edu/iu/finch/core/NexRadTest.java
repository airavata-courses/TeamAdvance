package edu.iu.finch.core;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by syodage on 9/11/16.
 */
public class NexRadTest {
    private AWSCredentials credentials;
    private NexRad nexRad;
    @BeforeMethod
    public void setUp() throws Exception {
        credentials = new AnonymousAWSCredentials();
        nexRad = new NexRad(credentials);
    }

    @Test
    public void testListBucket() throws Exception {
        List<S3ObjectSummary> summaries = nexRad.listBucket("2016/01/01/" + NexRadStation.FOP1);
        Assert.assertNotNull(summaries);
        Assert.assertEquals(summaries.size(), 156);
    }

    @Test
    public void testGetS3Object() throws Exception {
        Assert.assertTrue(true);
    }

}