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

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import edu.iu.finch.core.FinchException;
import edu.iu.finch.core.NexRad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private final NexRad nexRad;
    public Main() {
        nexRad = new NexRad();
    }


    public static void main(String[] args) throws IOException, FinchException {
        System.out.println("Hello from Finch!");
        Main main = new Main();
        String key = "2016/01/01/FOP1/FOP120160101_000203_V07.gz";
        List<Variable> variables = main.readAllVaribales(key);

        variables.stream().forEach(e -> System.out.println(e.getShortName()));
    }

    private List<Variable> readAllVaribales(String key) throws FinchException {
        NetcdfFile netcdfFile = nexRad.getNetcdfFile(key);
        return netcdfFile.getVariables();
    }

    private void printSummarCount(String key) {
        List<S3ObjectSummary> s3ObjectSummaries = nexRad.listBucket(key);
        log.debug("Summaries size : " + s3ObjectSummaries.size());
    }

    private void printBanner() {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Main.class.getResourceAsStream("banner.txt")))) {
            StringBuilder sb = new StringBuilder();
            String line = null;
            while((line = br.readLine()) !=null){
                sb.append(line).append('\n');
            }
            log.info(sb.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
