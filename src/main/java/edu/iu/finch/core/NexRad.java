/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package edu.iu.finch.core;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * This class represent the Amazon NEXRAD data set.
 */
public class NexRad {
    private AmazonS3 s3Client;
    private static final String BUCKET_NAME = "noaa-nexrad-level2";


    public NexRad() {
        init(new AnonymousAWSCredentials());
    }

    public NexRad(AWSCredentials credentials){
        init(credentials);
    }

    private void init(AWSCredentials credentials) {
        s3Client = new AmazonS3Client(credentials);
        Region region = Region.getRegion(Regions.US_EAST_1);
        s3Client.setRegion(region);
    }

    public List<S3ObjectSummary> listBucket(String key){
        ObjectListing listing = s3Client.listObjects(BUCKET_NAME, key);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        while (listing.isTruncated()) {
            listing = s3Client.listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
        }

        return summaries;
    }

    /**
     * key eg:  2015/05/15/KVWX/KVWX20150515_080737_V06.gz
     * @param key
     * @return
     */
    public S3Object getS3Object(String key) {
        S3Object s3object = s3Client.getObject(new GetObjectRequest(BUCKET_NAME, key ));
        System.out.println("Content Type - " + s3object.getObjectMetadata().getContentType());
        return s3object;
    }

    private void displayContent(S3ObjectInputStream objectContent) throws IOException {
        File file = new File("content.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        System.out.println(file.getAbsoluteFile());
        OutputStream osw = new FileOutputStream(file);
        byte[] buffer = new byte[4098];
        int n = -1;
        while((n = objectContent.read(buffer)) != -1) {
            osw.write(buffer, 0, n);
        }
        osw.close();
    }
}