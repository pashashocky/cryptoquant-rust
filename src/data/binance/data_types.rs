use casey::lower;
use std::fmt;
use std::path::Path;

macro_rules! pub_enum_str {
    (pub enum $name:ident {
        $($variant:ident),*,
    }) => {
        #[derive(Debug, PartialEq, Eq)]
        pub enum $name {
            $($variant),*
        }

        impl $name {
            #[doc = "Returns a lower string representation of the enum variant."]
            fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => lower!(stringify!($variant))),*
                }
            }
        }

        impl fmt::Display for $name {
            #[doc = "Displays the enum variant as a string."]
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.as_str())
            }
        }

        impl AsRef<Path> for $name {
            #[doc = "Returns a Path reference to the enum variant's string representation."]
            fn as_ref(&self) -> &Path {
                Path::new(self.as_str())
            }
        }
    };
}

pub_enum_str! {
    pub enum Asset {
        Futures,
        Option,
        Spot,
    }
}

pub_enum_str! {
    pub enum Cadence {
        Daily,
        Monthly,
    }
}

pub_enum_str! {
    pub enum DataType {
        AggTrades,
        KLines,
        Trades,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_str() {
        assert_eq!(Asset::Futures.as_str(), "futures");
        assert_eq!(Asset::Option.as_str(), "option");
        assert_eq!(Asset::Spot.as_str(), "spot");
        assert_eq!(Cadence::Daily.as_str(), "daily");
        assert_eq!(DataType::AggTrades.as_str(), "aggtrades");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Asset::Futures), "futures");
        assert_eq!(format!("{}", Cadence::Daily), "daily");
        assert_eq!(format!("{}", DataType::AggTrades), "aggtrades");
    }

    #[test]
    fn test_as_ref() {
        assert_eq!(Asset::Futures.as_ref(), Path::new("futures"));
        assert_eq!(Cadence::Daily.as_ref(), Path::new("daily"));
        assert_eq!(DataType::AggTrades.as_ref(), Path::new("aggtrades"));
    }
}
