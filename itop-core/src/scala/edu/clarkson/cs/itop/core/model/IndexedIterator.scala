package edu.clarkson.cs.itop.core.model

trait Index {

}

trait IndexableIterator[T] {

  def foreach(f: ((T, Index)) => Unit);

  def atIndex(index: Index): T;

  def first: (T, Index);

  def next(index: Index): (T, Index);
}