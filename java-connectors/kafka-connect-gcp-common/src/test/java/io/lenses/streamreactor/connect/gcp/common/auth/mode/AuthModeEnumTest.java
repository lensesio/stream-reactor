/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.gcp.common.auth.mode;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthModeEnumTest {

    @Test
    void testValueOfCaseInsensitiveOptional_validValue() {
        String validValue = "credentials";

        Optional<AuthModeEnum> result = AuthModeEnum.valueOfCaseInsensitiveOptional(validValue);

        assertTrue(result.isPresent());
        assertEquals(AuthModeEnum.CREDENTIALS, result.get());
    }

    @Test
    void testValueOfCaseInsensitiveOptional_validValueMixedCase() {
        String validValueMixedCase = "DeFaUlT";

        Optional<AuthModeEnum> result = AuthModeEnum.valueOfCaseInsensitiveOptional(validValueMixedCase);

        assertTrue(result.isPresent());
        assertEquals(AuthModeEnum.DEFAULT, result.get());
    }

    @Test
    void testValueOfCaseInsensitiveOptional_invalidValue() {
        String invalidValue = "invalid";

        Optional<AuthModeEnum> result = AuthModeEnum.valueOfCaseInsensitiveOptional(invalidValue);

        assertFalse(result.isPresent());
    }


    @Test
    void testValueOfCaseInsensitiveOptional_emptyValue() {
        String emptyValue = "";

        Optional<AuthModeEnum> result = AuthModeEnum.valueOfCaseInsensitiveOptional(emptyValue);

        assertFalse(result.isPresent());
    }
}
