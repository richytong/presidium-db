# Internal split right
```
{
  "4": {
    "leftChild": {
      "2": {
        "leftChild": {
          "1": {}
        },
        "rightChild": {
          "3": {}
        }
      }
    },
    "rightChild": {
      "6": {
        "leftChild": {
          "5": {}
        },
        "rightChild": {
          "7": {}
        }
      },
      "8": {
        "leftChild": {
          "7": {}
        },
        "rightChild": {
          "9": {}
        }
      },
      "10": {
        "leftChild": {
          "9": {}
        },
        "rightChild": {
          "11": {},
          "12": {}
        }
      }
    }
  }
}
```

```
{
  "4": {
    "leftChild": {
      "2": {
        "leftChild": {
          "1": {}
        },
        "rightChild": {
          "3": {}
        }
      }
    },
    "rightChild": {
      "6": {
        "leftChild": {
          "5": {}
        },
        "rightChild": {
          "7": {}
        }
      },
    }
  },
  "8": {
    "leftChild": {
      "6": {
        "leftChild": {
          "5": {}
        },
        "rightChild": {
          "7": {}
        }
      },
    },
    "rightChild": {
      "10": {
        "leftChild": {
          "9": {}
        },
        "rightChild": {
          "11": {},
          "12": {}
        }
      }
    }
  }
}
```

# Leaf split right

Example 1:
```
{
  "2": {
    "leftChild": {
      "1": {}
    },
    "rightChild": {
      "3": {},
      "30": {},
      "31": {}
    }
  },
  "32": {
    "leftChild": {
      "3": {},
      "30": {},
      "31": {}
    },
    "rightChild": {
      "33": {}
    }
  }
}
```

```
{
  "2": {
    "leftChild": {
      "1": {}
    },
    "rightChild": {
      "3": {},
    }
  },
  "30": {
    "leftChild": {
      "3": {}
    },
    "rightChild": {
      "31": {}
    }
  },
  "32": {
    "leftChild": {
      "31": {}
    },
    "rightChild": {
      "33": {}
    }
  }
}
```

Example 2:
```
{
  "96": {
    "leftChild": {
      "26": {
        "leftChild": {
          "7": {}
        },
        "rightChild": {
          "55": {},
          "58": {},
          "90": {}
        }
      }
    },
    "rightChild": {
      "101": {
        "leftChild": {
          "98": {}
        },
        "rightChild": {
          "116": {},
          "127": {}
        }
      }
    }
  }
}
```

```
{
  "96": {
    "leftChild": {
      "26": {
        "leftChild": {
          "7": {}
        },
        "rightChild": {
          "55": {},
        }
      },
      "58": {
        "leftChild": {
          "55": {},
        },
        "rightChild": {
          "90": {}
        }
      }
    },
    "rightChild": {
      "101": {
        "leftChild": {
          "98": {}
        },
        "rightChild": {
          "116": {},
          "127": {}
        }
      }
    }
  }
}
```

Example 3:
```
{
  "46": {
    "leftChild": {
      "29": {
        "leftChild": {
          "14": {},
          "20": {},
          "24": {}
        },
        "rightChild": {
          "33": {},
          "35": {}
        }
      }
    },
    "rightChild": {
      "90": {
        "leftChild": {
          "51": {},
          "67": {},
          "68": {}
        },
        "rightChild": {
          "91": {},
          "93": {},
          "125": {}
        }
      }
    }
  }
}
```

```
{
  "46": {
    "leftChild": {
      "29": {
        "leftChild": {
          "14": {},
          "20": {},
          "24": {}
        },
        "rightChild": {
          "33": {},
          "35": {}
        }
      }
    },
    "rightChild": {
      "90": {
        "leftChild": {
          "51": {},
          "67": {},
          "68": {}
        },
        "rightChild": {
          "91": {}
        }
      },
      "93": {
        "leftChild": {
          "91": {}
        },
        "rightChild": {
          "125": {}
        }
      }
    }
  }
}
```

# Leaf split left

```
{
  "32": {
    "leftChild": {
      "29": {},
      "30": {},
      "31": {}
    },
    "rightChild": {
      "33": {}
    }
  }
}
```

```
{
  "30": {
    "leftChild": {
      "29": {},
    },
    "rightChild": {
      "31": {}
    }
  },
  "32": {
    "leftChild": {
      "31": {}
    },
    "rightChild": {
      "33": {}
    }
  }
}
```

# Internal split left
```
{
  "29": {
    "leftChild": {
      "23": {
        "leftChild": {
          "21": {},
          "22": {}
        },
        "rightChild": {
          "24": {}
        }
      },
      "25": {
        "leftChild": {
          "24": {}
        },
        "rightChild": {
          "26": {}
        }
      },
      "27": {
        "leftChild": {
          "26": {}
        },
        "rightChild": {
          "28": {}
        }
      }
    },
    "rightChild": {
      "31": {
        "leftChild": {
          "30": {}
        },
        "rightChild": {
          "32": {},
          "33": {}
        }
      }
    }
  }
}
```

```
{
  "25": {
    "leftChild": {
      "23": {
        "leftChild": {
          "21": {},
          "22": {}
        },
        "rightChild": {
          "24": {}
        }
      },
    },
    "rightChild": {
      "27": {
        "leftChild": {
          "26": {}
        },
        "rightChild": {
          "28": {}
        }
      }
    }
  },
  "29": {
    "leftChild": {
      "27": {
        "leftChild": {
          "26": {}
        },
        "rightChild": {
          "28": {}
        }
      }
    },
    "rightChild": {
      "31": {
        "leftChild": {
          "30": {}
        },
        "rightChild": {
          "32": {},
          "33": {}
        }
      }
    }
  }
}
```
