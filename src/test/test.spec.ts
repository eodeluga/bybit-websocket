import { describe } from 'mocha';
import assert from 'chai';
import * as fs from 'fs';

describe('Test template', function () {
    it('has a test folder', function () {
        const fileExists = fs.existsSync('src/test');
        assert.expect(fileExists).be.true;
    })
})