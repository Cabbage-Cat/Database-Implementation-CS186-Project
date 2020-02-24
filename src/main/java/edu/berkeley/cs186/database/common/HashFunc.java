package edu.berkeley.cs186.database.common;

import java.util.function.Function;
import edu.berkeley.cs186.database.databox.DataBox;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;;

public class HashFunc {
    private static BigInteger p = BigInteger.valueOf(18618618661L); // Arbitrary prime > 32 bits
    private static BigInteger m = BigInteger.valueOf(1L << 32L); // 1 + max size of unsigned 32-bit
    private static Random generator = new Random();

    public static Function<DataBox, Integer> getHashFunction(int pass) {
        assert(pass >= 1);
        if (pass == 1) {
            // First pass just uses regular hash function
            return (DataBox d) -> { return d.hashCode();};
        }
        // Future passes select a random fine-grain hash function
        generator.setSeed(pass);
        final BigInteger a = generateBigA(p);
        final BigInteger b = generateBoundedBigInteger(p);

        // Take 170 to learn more about what's going on here!
        // https://en.wikipedia.org/wiki/Universal_hashing#Hashing_integers
        return (DataBox d) -> {
            BigInteger bhash = BigInteger.valueOf(d.hashCode());
            BigInteger result = a.multiply(bhash).add(b).mod(p).mod(m);
            // Don't tell Nick Weaver, but we do an implicit
            // unsigned -> signed conversion here. Shhh...
            return result.intValue();
        };
    }

    /**
     * Generate a BigInteger between 0 and bound right exclusive.
     */
    private static BigInteger generateBoundedBigInteger(BigInteger bound) {
        BigDecimal bbound = new BigDecimal(bound);
        BigDecimal scalar = BigDecimal.valueOf(generator.nextFloat());
        BigDecimal scaled = bbound.multiply(scalar);
        return scaled.toBigInteger();
    }

    /**
     * Same as above but in the highly unfortunate case a == 0 we reroll it
     */
    private static BigInteger generateBigA(BigInteger bound) {
        BigInteger a;
        do {
            a = generateBoundedBigInteger(p);
        } while (a.equals(BigInteger.ZERO));
        return a; // We can't let a be zero! Think about why...
    }
}