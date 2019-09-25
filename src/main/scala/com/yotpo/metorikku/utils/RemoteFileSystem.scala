package com.yotpo.metorikku.utils

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3URI

import scala.io.{BufferedSource, Source}

trait RemoteFileSystem extends FileSystem {
  override def read(path: String): BufferedSource = {
    val uri: AmazonS3URI = new AmazonS3URI(path)
    import com.amazonaws.auth.AWSStaticCredentialsProvider
    import com.amazonaws.services.s3.AmazonS3ClientBuilder

    val creds = new EnvironmentVariableCredentialsProvider().getCredentials
    val s3Client = AmazonS3ClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(creds)).build

    val s3Object = s3Client.getObject(uri.getBucket, uri.getKey)
    Source.fromInputStream(s3Object.getObjectContent)
  }
}