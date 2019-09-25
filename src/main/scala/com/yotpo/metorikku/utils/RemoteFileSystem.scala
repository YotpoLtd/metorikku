package com.yotpo.metorikku.utils

import java.io.File

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3URI
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import scala.io.{BufferedSource, Source}

trait RemoteFileSystem extends FileSystem {
  override def read(path: String): BufferedSource = {
    try {
      val uri: AmazonS3URI = new AmazonS3URI(path)

      val creds = new EnvironmentVariableCredentialsProvider().getCredentials
      val s3Client = AmazonS3ClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(creds)).build

      val s3Object = s3Client.getObject(uri.getBucket, uri.getKey)
      Source.fromInputStream(s3Object.getObjectContent)
    }
    catch {
      case e: Exception => throw MetorikkuException(e.getMessage)
    }
  }

  override def isAbsolute(path: String): Boolean = {
    true
  }

  override def baseDir(path: String): String = {
    path.substring(0,path.lastIndexOf("/"))
  }
}