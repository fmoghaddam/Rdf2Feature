package util;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;

/**
 * Config file reader
 * 
 * @author fbm
 *
 */
public class Config {
	private static final String BUNDLE_NAME = "config";

	private static final Logger LOG = Logger.getLogger(Config.class.getSimpleName());

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(Config.BUNDLE_NAME);

	/**
	 * Private constructor to prevent creation with new.
	 */
	private Config() {
	}

	/**
	 * Returns a boolean from the config.properties file.
	 * 
	 * @param key        to read the configuration for
	 * @param defBoolean default boolean being returned, if no value is found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static boolean getBoolean(final String key, final boolean defBoolean) {
		try {
			return Boolean.parseBoolean(Config.RESOURCE_BUNDLE.getString(key));
		} catch (final Exception e) {
			Config.LOG.warn(e.getMessage(), e);
			return defBoolean;
		}
	}

	/**
	 * Returns an integer from the config.properties file.
	 * 
	 * @param key    to read the configuration for
	 * @param defInt default integer being returned, if no value is found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static int getInt(final String key, final int defInt) {
		try {
			return Integer.parseInt(Config.RESOURCE_BUNDLE.getString(key));
		} catch (final Exception e) {
			Config.LOG.warn(e.getMessage());
			return defInt;
		}
	}

	/**
	 * Returns an float from the config.properties file.
	 * 
	 * @param key      to read the configuration for
	 * @param defFloat default float value being returned, if no value is found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static float getFloat(final String key, final float defFloat) {
		try {
			return Float.parseFloat(Config.RESOURCE_BUNDLE.getString(key));
		} catch (final Exception e) {
			Config.LOG.warn(e.getMessage());
			return defFloat;
		}
	}

	/**
	 * Returns an integer from the config.properties file.
	 * 
	 * @param key       to read the configuration for
	 * @param defDouble default double value being returned, if no value is found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static double getDouble(final String key, final double defDouble) {
		try {
			return Double.parseDouble(Config.RESOURCE_BUNDLE.getString(key));
		} catch (final Exception e) {
			Config.LOG.warn(e.getMessage());
			return defDouble;
		}
	}

	/**
	 * A wrapper for getAddress(StringString)
	 * 
	 * @see #getAddress(String,String)
	 * @param key            to read the configuration for
	 * @param defaultAddress default address value being returned, if no value is
	 *                       found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static int getAddress(final String key, final int defaultAddress) {
		return Config.getAddress(key, Integer.toString(defaultAddress));
	}

	/**
	 * Returns an integer from the config.properties file expected to be an address.
	 * Several notations may apply for an address:
	 * <ul>
	 * <li>plain integer
	 * <li>hexadecimal notation beginning with '0x' or beginning with '0X'
	 * <li>byte wise notation with bytes separated by '/', starting with the most
	 * significant byte
	 * </ul>
	 * 
	 * Samples:
	 * <ul>
	 * <li>2571
	 * <li>0x0a0b
	 * <li>11/10
	 * </ul>
	 * 
	 * @param key            to read the configuration for
	 * @param defaultAddress default address being returned, if no value is found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static int getAddress(final String key, final String defaultAddress) {

		String content;
		try {
			content = Config.RESOURCE_BUNDLE.getString(key);
		} catch (final Exception e) {
			Config.LOG.warn(e.getMessage());
			content = defaultAddress;
		}

		content = content.trim();

		try {
			if (content.startsWith("0x") || content.startsWith("0X")) {
				// read as hex value
				return Integer.decode(content);
			} else if (content.contains("/")) {
				// read as two decimal byte values
				String[] bytes = content.split("/");
				int nBytes = bytes.length;
				int address = 0;
				for (int i = 0; i < nBytes; i++) {
					int val = Integer.parseInt(bytes[i]);
					address = address << 8;
					address |= val;
				}
				return address;
			} else {
				// read as a integer value
				return Integer.parseInt(content);
			}

		} catch (final Exception e) {
			Config.LOG.warn(e.getMessage());
			return 0;
		}
	}

	/**
	 * Returns a long integer from the config.properties file.
	 * 
	 * @param key     to read the configuration for
	 * @param defLong default long integer being returned, if no value is found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static long getLong(final String key, final long defLong) {
		try {
			return Long.parseLong(Config.RESOURCE_BUNDLE.getString(key));
		} catch (final Exception e) {
			Config.LOG.warn(e.getMessage());
			return defLong;
		}
	}

	/**
	 * Returns a string from the config.properties file.
	 * 
	 * @param key       to read the configuration for
	 * @param defString default string being returned, if no value is found
	 * @return the according configured value; the default value, if nothing is
	 *         configured for the provided key
	 */
	public static String getString(final String key, final String defString) {
		try {
			return Config.RESOURCE_BUNDLE.getString(key);
		} catch (final MissingResourceException e) {
			Config.LOG.warn(e.getMessage());
			return defString;
		}
	}

}
