package edu.clarkson.cs.itop.scheduler

import java.util.concurrent.ThreadFactory

class TaskThreadFactory extends ThreadFactory {

  def newThread(run: Runnable): Thread = {
    return null;
  }
}