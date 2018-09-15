/*
 * Copyright (C) 2018 Michail Klimenkov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package klim.dclined;

/**
 * Instances of this exception are thrown when a commit is attempted on a transaction,
 * but a conflict with any other transaction has occured.
 *
 * @author Michail Klimenkov
 */
public class TransactionAbortedException extends RuntimeException {
    public TransactionAbortedException(String message) {
        super(message);
    }
}
