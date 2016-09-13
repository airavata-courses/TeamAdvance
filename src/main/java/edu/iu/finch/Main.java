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
package edu.iu.finch;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import edu.iu.finch.core.NexRad;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello from Finch!");
        NexRad nexRad = new NexRad();

        List<S3ObjectSummary> summaries = nexRad.listBucket("2016/01/01/FOP1");
        System.out.println(summaries.size());
        System.out.println(summaries.get(0));

        S3Object s3Object = nexRad.getS3Object("2016/01/01/FOP1/FOP120160101_000203_V07.gz");
        System.out.println(s3Object);
//        System.out.println(s3Object.getObjectMetadata());
        S3ObjectInputStream s3InputStream = s3Object.getObjectContent();

        //print(s3InputStream);

    }

    private static void print(S3ObjectInputStream s3InputStream) throws IOException {
        byte[] buff = new byte[1024];
        ByteArrayOutputStream original = new ByteArrayOutputStream();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        int len = -1;
        while((len = s3InputStream.read(buff)) != -1){
            original.write(buff, 0, len);
        }
        GZIPInputStream gunzip = new GZIPInputStream(new ByteArrayInputStream(original.toByteArray()));
        original.close();

        while ((len = gunzip.read(buff)) != -1) {
            outputStream.write(buff, 0, len);
        }
        System.out.println(new String(outputStream.toByteArray()));
    }
}
