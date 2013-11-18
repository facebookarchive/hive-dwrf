package com.facebook.hive.orc.lazy;

import org.apache.hadoop.io.Text;

public class OrcLazyString extends OrcLazyObject {

  public OrcLazyString(LazyStringTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyString(OrcLazyString copy) {
    super(copy);
    previous = new Text((Text)copy.previous);
  }
}
